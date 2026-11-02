// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package native

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ballerina-nutcracker/ballerina/decimal"
	"github.com/ballerina-nutcracker/ballerina/runtime"
	"github.com/ballerina-nutcracker/ballerina/runtime/extern"
	"github.com/ballerina-nutcracker/ballerina/semtypes"
	"github.com/ballerina-nutcracker/ballerina/values"
)

// engine runs workflow instances in process, without durability: a workflow is a strand,
// an activity is a direct call, and data events complete the futures of the events record.
type engine struct {
	rt            *runtime.Runtime
	mu            sync.Mutex
	instances     map[string]*instance
	nextID        atomic.Uint64
	tasks         map[string]*humanTask
	nextTaskID    atomic.Uint64
	anydataListTy semtypes.SemType
	anydataListAt *semtypes.ListAtomicType
	anydataMapTy  semtypes.SemType
	anydataMapAt  *semtypes.MappingAtomicType
}

type instance struct {
	id           string
	workflowType string
	parent       string
	events       map[string]*values.Future
	done         chan struct{}
	result       values.BalValue
	completions  []*humanTask
}

func newEngine(rt *runtime.Runtime) *engine {
	env := rt.GetTypeEnv()
	anydata := semtypes.CreateAnydata(semtypes.ContextFrom(env))
	listDef := semtypes.NewListDefinition()
	listTy := listDef.Define(env, nil, semtypes.ListRest(anydata))
	mapDef := semtypes.NewMappingDefinition()
	mapTy := mapDef.Define(env, nil, anydata)
	return &engine{rt: rt, instances: make(map[string]*instance), tasks: make(map[string]*humanTask),
		anydataListTy: listTy, anydataListAt: semtypes.ToListAtomicType(env, listTy),
		anydataMapTy: mapTy, anydataMapAt: semtypes.ToMappingAtomicType(semtypes.ContextFrom(env), mapTy)}
}

func (e *engine) run(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	return e.start(ctx, args[0].(*values.Function), args[1], "", "", nil)
}

// runWithId starts under a caller-chosen id. The policies decide what happens when the id is held:
// a running holder is joined or refused, a closed one is replaced or refused.
func (e *engine) runWithId(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	id := args[1].(string)
	ifRunning, ifClosed := args[3].(string), args[4].(string)
	admit := func(holder *instance) values.BalValue {
		status := holder.status()
		switch {
		case status == "RUNNING" && ifRunning == "USE_EXISTING":
			return holder.id
		case status == "RUNNING":
		case ifClosed == "ALLOW_DUPLICATE":
			return nil
		case ifClosed == "ALLOW_DUPLICATE_FAILED_ONLY" && status == "FAILED":
			return nil
		}
		return moduleError(ctx, "newInstanceAlreadyExistsError", id, status)
	}
	return e.start(ctx, args[0].(*values.Function), args[2], "", id, admit)
}

// start creates an instance of fn with the input and runs it on its own strand. parent is the
// id of the workflow that started it, empty for a top-level instance; id is the caller's choice
// or empty for a generated one; admit, when given, answers for an id already held, with a
// result to return instead of starting, or nil to replace the holder.
func (e *engine) start(ctx *extern.Context, fn *values.Function, input values.BalValue,
	parent, id string, admit func(holder *instance) values.BalValue) (values.BalValue, error) {
	org, module, name := splitLookupKey(fn.LookupKey)
	handle, ok := ctx.LookupFunction(org, module, name)
	if !ok {
		return values.NewErrorWithMessage("workflow function not found: " + fn.LookupKey), nil
	}
	sig, ok := ctx.FunctionSignature(handle)
	if !ok {
		return values.NewErrorWithMessage("workflow function has no signature: " + fn.LookupKey), nil
	}
	if id == "" {
		id = fmt.Sprintf("%s-%d", name, e.nextID.Add(1))
	}
	inst := &instance{
		id:           id,
		workflowType: name,
		parent:       parent,
		events:       make(map[string]*values.Future),
		done:         make(chan struct{}),
	}
	callArgs, errVal := e.workflowArgs(ctx, inst, sig, input)
	if errVal != nil {
		return errVal, nil
	}
	e.mu.Lock()
	if holder, held := e.instances[inst.id]; held && admit != nil {
		if answer := admit(holder); answer != nil {
			e.mu.Unlock()
			return answer, nil
		}
	}
	e.instances[inst.id] = inst
	e.mu.Unlock()
	go func() {
		strand := e.rt.AcquirePooledContext()
		defer e.rt.ReleasePooledContext(strand)
		defer close(inst.done)
		defer func() {
			// A panicking workflow fails its instance instead of taking the process down
			if rec := recover(); rec != nil {
				strand.ReleaseAllHeldLocks()
				inst.result = panicAsError(rec)
			}
		}()
		result, err := strand.InvokeFunctionValue(fn, callArgs)
		if err != nil {
			result = values.NewErrorWithMessage(err.Error())
		}
		inst.result = result
	}()
	return inst.id, nil
}

// workflowArgs binds each workflow parameter by its type: the Context object, the events
// record of futures, or the input.
func (e *engine) workflowArgs(ctx *extern.Context, inst *instance, sig extern.FunctionSignature,
	input values.BalValue) ([]values.BalValue, *values.Error) {
	cx := ctx.TypeCtx()
	contextType, errVal := contextType(ctx)
	if errVal != nil {
		return nil, errVal
	}
	callArgs := make([]values.BalValue, len(sig.Params))
	for i, p := range sig.Params {
		switch {
		case !semtypes.IsNever(p.Type) && semtypes.IsSubtype(cx, p.Type, contextType):
			wfCtx, errVal := callModuleFunction(ctx, "newContext", inst)
			if errVal != nil {
				return nil, errVal
			}
			callArgs[i] = wfCtx
		case isEventsRecord(cx, p.Type):
			callArgs[i] = newEventsRecord(cx, inst, p.Type)
		default:
			converted, convErr := values.CloneWithType(cx, input, p.Type)
			if convErr != nil {
				return nil, convErr
			}
			callArgs[i] = converted
		}
	}
	return callArgs, nil
}

// contextType is the module's Context class, read off the factory that builds one.
func contextType(ctx *extern.Context) (semtypes.SemType, *values.Error) {
	factory, ok := ctx.LookupFunction(orgName, moduleName, "newContext")
	if !ok {
		return semtypes.Never, values.NewErrorWithMessage("workflow:newContext not found")
	}
	sig, ok := ctx.FunctionSignature(factory)
	if !ok {
		return semtypes.Never, values.NewErrorWithMessage("workflow:newContext has no signature")
	}
	return sig.ReturnType, nil
}

// callModuleFunction invokes a module-level Ballerina function that builds what Go cannot: the
// Context object and the module's typed errors.
func callModuleFunction(ctx *extern.Context, name string, args ...values.BalValue) (values.BalValue, *values.Error) {
	fn, ok := ctx.LookupFunction(orgName, moduleName, name)
	if !ok {
		return nil, values.NewErrorWithMessage("workflow:" + name + " not found")
	}
	result, err := ctx.InvokeFunction(fn, args)
	if err != nil {
		return nil, values.NewErrorWithMessage(err.Error())
	}
	return result, nil
}

// moduleError builds one of the module's typed errors through its Ballerina factory.
func moduleError(ctx *extern.Context, factory string, args ...values.BalValue) values.BalValue {
	errVal, failed := callModuleFunction(ctx, factory, args...)
	if failed != nil {
		return failed
	}
	return errVal
}

func panicAsError(rec any) *values.Error {
	switch v := rec.(type) {
	case *values.Error:
		return v
	case error:
		return values.NewErrorWithMessage(v.Error())
	default:
		return values.NewErrorWithMessage(fmt.Sprintf("%v", rec))
	}
}

func isEventsRecord(cx semtypes.Context, ty semtypes.SemType) bool {
	if !semtypes.IsSubtype(cx, ty, semtypes.Mapping) {
		return false
	}
	atomic := semtypes.ToMappingAtomicType(cx, ty)
	if atomic == nil || len(atomic.FieldNames()) == 0 {
		return false
	}
	for _, name := range atomic.FieldNames() {
		if !semtypes.IsSubtype(cx, atomic.FieldInnerVal(name), semtypes.Future) {
			return false
		}
	}
	return true
}

func newEventsRecord(cx semtypes.Context, inst *instance, ty semtypes.SemType) *values.Map {
	atomic := semtypes.ToMappingAtomicType(cx, ty)
	entries := make([]values.MapEntry, 0, len(atomic.FieldNames()))
	for _, name := range atomic.FieldNames() {
		future := values.NewFuture(atomic.FieldInnerVal(name))
		inst.events[name] = future
		entries = append(entries, values.MapEntry{Key: name, Value: future})
	}
	return values.NewMap(ty, atomic, false, entries)
}

func (e *engine) lookup(ctx *extern.Context, id string) (*instance, values.BalValue) {
	e.mu.Lock()
	defer e.mu.Unlock()
	inst, ok := e.instances[id]
	if !ok {
		return nil, moduleError(ctx, "newInstanceNotFoundError", id)
	}
	return inst, nil
}

func (e *engine) sendData(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	return e.deliver(ctx, args[1].(string), args[2].(string), args[3]), nil
}

// deliver completes the named event future of the instance with data converted to its type.
func (e *engine) deliver(ctx *extern.Context, id, dataName string, data values.BalValue) values.BalValue {
	inst, errVal := e.lookup(ctx, id)
	if errVal != nil {
		return errVal
	}
	future, ok := inst.events[dataName]
	if !ok {
		return values.NewErrorWithMessage("workflow has no data event: " + dataName)
	}
	cx := ctx.TypeCtx()
	converted, convErr := values.CloneWithType(cx, data, semtypes.FutureEventualType(cx, future.Type))
	if convErr != nil {
		return convErr
	}
	future.Complete(converted, nil)
	return nil
}

func (e *engine) getStatus(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	inst, errVal := e.lookup(ctx, args[0].(string))
	if errVal != nil {
		return errVal, nil
	}
	return inst.status(), nil
}

func (inst *instance) status() string {
	select {
	case <-inst.done:
		if _, failed := inst.result.(*values.Error); failed {
			return "FAILED"
		}
		return "COMPLETED"
	default:
		return "RUNNING"
	}
}

func (e *engine) getResult(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	inst, errVal := e.lookup(ctx, args[0].(string))
	if errVal != nil {
		return errVal, nil
	}
	select {
	case <-inst.done:
		return instanceResult(ctx, inst, args[1].(*values.TypeDesc)), nil
	default:
		return moduleError(ctx, "newInProgressError", inst.id), nil
	}
}

func (e *engine) waitForResult(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	inst, errVal := e.lookup(ctx, args[0].(string))
	if errVal != nil {
		return errVal, nil
	}
	timeout, boundErr := durationMillis(args[2], "a timeout")
	if boundErr != nil {
		return boundErr, nil
	}
	return awaitResult(ctx, inst, args[1].(*values.TypeDesc), timeout), nil
}

// awaitResult waits up to timeout milliseconds (negative for no bound) for the instance to close
// and answers its result; past the bound it answers WorkflowInProgressError.
func awaitResult(ctx *extern.Context, inst *instance, td *values.TypeDesc, timeout int64) values.BalValue {
	// Poll between yields, as runtime:sleep does: blocking on the channel would hold this
	// strand's thread, and a strand sharing it could be the one that sends the data.
	clock := ctx.Env.Platform.Time
	deadline := clock.MonotonicNow() + time.Duration(timeout)*time.Millisecond
	for {
		select {
		case <-inst.done:
			return instanceResult(ctx, inst, td)
		default:
		}
		if timeout >= 0 && clock.MonotonicNow() >= deadline {
			return moduleError(ctx, "newInProgressError", inst.id)
		}
		<-ctx.Yield()
	}
}

// Child workflows: a child is an instance started from a workflow's strand with the parent
// recorded. Nothing cancels it when the parent closes, so a child outlives its parent.

func (e *engine) runChildWorkflow(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	return e.start(ctx, args[1].(*values.Function), args[2], parentOf(args[0]).id, "", nil)
}

func (e *engine) getChildWorkflowResult(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	return e.getResult(ctx, args[1:])
}

func (e *engine) waitForChildWorkflow(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	inst, errVal := e.lookup(ctx, args[1].(string))
	if errVal != nil {
		return errVal, nil
	}
	return awaitResult(ctx, inst, args[2].(*values.TypeDesc), -1), nil
}

func (e *engine) callWorkflow(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	started, _ := e.start(ctx, args[1].(*values.Function), args[2], parentOf(args[0]).id, "", nil)
	id, ok := started.(string)
	if !ok {
		return started, nil
	}
	inst, _ := e.lookup(ctx, id)
	return awaitResult(ctx, inst, args[3].(*values.TypeDesc), -1), nil
}

func (e *engine) sendDataToChildWorkflow(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	return e.deliver(ctx, args[1].(string), args[2].(string), args[3]), nil
}

// parentOf reads the instance behind a Context object's native handle.
func parentOf(self values.BalValue) *instance {
	handle, _ := self.(*values.Object).Get("nativeContext")
	return handle.(*instance)
}

// durationMillis converts a workflow:Duration record to milliseconds; nil means no bound.
func durationMillis(duration values.BalValue, what string) (int64, *values.Error) {
	if duration == nil {
		return -1, nil
	}
	fields := duration.(*values.Map)
	intField := func(name string) int64 {
		v, ok := fields.Get(name)
		if !ok {
			return 0
		}
		return v.(int64)
	}
	if intField("years") != 0 || intField("months") != 0 {
		return 0, values.NewErrorWithMessage(what + " does not support 'years' or 'months': they have no fixed length")
	}
	seconds := intField("weeks")*604800 + intField("days")*86400 + intField("hours")*3600 + intField("minutes")*60
	millis := seconds * 1000
	if v, ok := fields.Get("seconds"); ok {
		millis += int64(v.(*decimal.Decimal).Float64() * 1000)
	}
	if millis < 0 {
		return 0, values.NewErrorWithMessage(what + " must not be negative")
	}
	return millis, nil
}

// instanceResult is what a result read answers for a closed instance: the result as T, or an
// InstanceFailedError carrying the workflow's own error.
func instanceResult(ctx *extern.Context, inst *instance, td *values.TypeDesc) values.BalValue {
	if errVal, ok := inst.result.(*values.Error); ok {
		return moduleError(ctx, "newInstanceFailedError", inst.id, errVal)
	}
	return convertResult(ctx, inst.result, td)
}

func convertResult(ctx *extern.Context, result values.BalValue, td *values.TypeDesc) values.BalValue {
	if errVal, ok := result.(*values.Error); ok {
		return errVal
	}
	converted, convErr := values.CloneWithType(ctx.TypeCtx(), result, td.Constraint())
	if convErr != nil {
		return convErr
	}
	return converted
}

// callActivity runs the activity on the workflow's strand, binding named args to parameters,
// under the retry policy of its options.
func (e *engine) callActivity(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	fn := args[1].(*values.Function)
	named := args[2].(*values.Map)
	td := args[3].(*values.TypeDesc)
	options := args[5].(*values.Map)
	org, module, name := splitLookupKey(fn.LookupKey)
	plan, policyErr := readRetryPolicy(ctx, field(options, "retryPolicy"), name)
	if policyErr != nil {
		return policyErr, nil
	}
	if gateErr := readApprovalPolicy(ctx, field(options, "approvalPolicy"), name); gateErr != nil {
		return gateErr, nil
	}
	handle, ok := ctx.LookupFunction(org, module, name)
	if !ok {
		return values.NewErrorWithMessage("activity function not found: " + fn.LookupKey), nil
	}
	sig, ok := ctx.FunctionSignature(handle)
	if !ok {
		return values.NewErrorWithMessage("activity function has no signature: " + fn.LookupKey), nil
	}
	paramNames := make(map[string]bool, len(sig.Params))
	for _, p := range sig.Params {
		paramNames[p.Name] = true
	}
	for _, key := range named.Keys() {
		if !paramNames[key] {
			return values.NewErrorWithMessage("unknown activity argument: " + key), nil
		}
	}
	cx := ctx.TypeCtx()
	callArgs := make([]values.BalValue, len(sig.Params))
	for i, p := range sig.Params {
		v, present := named.Get(p.Name)
		switch {
		case present:
			callArgs[i] = v
		case !semtypes.IsNever(p.Type) && semtypes.IsSubtype(cx, p.Type, semtypes.Typedesc):
			// A typedesc parameter left unnamed takes the call's T, as the built-in activities expect
			callArgs[i] = td
		default:
			return values.NewErrorWithMessage("missing activity argument: " + p.Name), nil
		}
	}
	return runWithRetries(ctx, plan, name, func() values.BalValue {
		return invokeActivity(ctx, fn, callArgs, td)
	}), nil
}

// invokeActivity is one attempt: a panic comes back as an error, as a failed activity does.
func invokeActivity(ctx *extern.Context, fn *values.Function, callArgs []values.BalValue,
	td *values.TypeDesc) (result values.BalValue) {
	defer func() {
		if rec := recover(); rec != nil {
			result = panicAsError(rec)
		}
	}()
	callResult, callErr := ctx.InvokeFunctionValue(fn, callArgs)
	if callErr != nil {
		return values.NewErrorWithMessage(callErr.Error())
	}
	return convertResult(ctx, callResult, td)
}
