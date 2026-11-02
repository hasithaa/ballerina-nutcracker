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
	"strings"
	"time"

	"github.com/ballerina-nutcracker/ballerina/runtime/extern"
	"github.com/ballerina-nutcracker/ballerina/semtypes"
	"github.com/ballerina-nutcracker/ballerina/values"
)

// humanTask is a decision a workflow waits on until someone outside completes it.
type humanTask struct {
	id, name, parent string
	audience         taskAudience
	resultType       semtypes.SemType
	done             chan struct{}
	closed           bool
	result           values.BalValue
	completedBy      values.BalValue
	completedAt      values.BalValue
}

type taskAudience struct {
	userRoles, users, excludedUsers, excludedRoles []string
}

// awaitHumanTask creates the task from its definition and waits, between yields, until it is
// completed or its bound passes.
func (e *engine) awaitHumanTask(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	parent := parentOf(args[0])
	taskName := args[1].(string)
	td := args[3].(*values.TypeDesc)
	definition := args[5].(*values.Map)
	failed := func(msg string) values.BalValue { return moduleError(ctx, "newHumanTaskFailedError", msg) }
	if strings.TrimSpace(taskName) == "" || strings.ContainsAny(taskName, ".|") {
		return failed("Human task name must not be blank or contain '.' or '|': '" + taskName + "'"), nil
	}
	audience := taskAudience{
		userRoles:     stringsOf(field(definition, "userRoles")),
		users:         stringsOf(field(definition, "users")),
		excludedUsers: stringsOf(field(definition, "excludedUsers")),
		excludedRoles: stringsOf(field(definition, "excludedRoles")),
	}
	if len(audience.userRoles) == 0 && len(audience.users) == 0 {
		return failed("Human task '" + taskName + "' must name 'userRoles' or 'users'"), nil
	}
	if inputType, ok := field(definition, "taskInputType").(*values.TypeDesc); ok {
		if _, convErr := values.CloneWithType(ctx.TypeCtx(), args[2], inputType.Constraint()); convErr != nil {
			return failed("Human task '" + taskName + "' input does not match its taskInputType: " + convErr.Message), nil
		}
	}
	timeout, boundErr := durationMillis(field(definition, "timeout"), "a task timeout")
	if boundErr != nil {
		return failed(boundErr.Message), nil
	}
	task := &humanTask{id: fmt.Sprintf("humantask-%d", e.nextTaskID.Add(1)), name: taskName, parent: parent.id,
		audience: audience, resultType: td.Constraint(), done: make(chan struct{})}
	e.mu.Lock()
	e.tasks[task.id] = task
	e.mu.Unlock()
	clock := ctx.Env.Platform.Time
	deadline := clock.MonotonicNow() + time.Duration(timeout)*time.Millisecond
	for {
		select {
		case <-task.done:
			parent.completions = append(parent.completions, task)
			return convertResult(ctx, task.result, td), nil
		default:
		}
		if timeout >= 0 && clock.MonotonicNow() >= deadline && e.closeTask(task) {
			return moduleError(ctx, "newHumanTaskTimeoutError", taskName, task.id, isoDuration(timeout),
				clock.Now().UTC().Format(time.RFC3339Nano)), nil
		}
		<-ctx.Yield()
	}
}

// closeTask closes the task once; false means a completion or a timeout got there first.
func (e *engine) closeTask(task *humanTask) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if task.closed {
		return false
	}
	task.closed = true
	close(task.done)
	return true
}

// completeHumanTask answers a pending task with the caller's result, after the audience check
// and the result type the workflow expects. The receipt names the task and its workflow.
func (e *engine) completeHumanTask(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	id := args[0].(string)
	e.mu.Lock()
	task, ok := e.tasks[id]
	e.mu.Unlock()
	if !ok {
		return values.NewErrorWithMessage("Human task '" + id + "' not found"), nil
	}
	roles, rolesGiven := args[2].(*values.List)
	user, userGiven := args[3].(string)
	if msg := task.audience.denial(stringsOf(roles), rolesGiven, user, userGiven, "task '"+id+"'"); msg != "" {
		return values.NewErrorWithMessage(msg), nil
	}
	converted, convErr := values.CloneWithType(ctx.TypeCtx(), args[1], task.resultType)
	if convErr != nil {
		return values.NewErrorWithMessage("Human task '" + id + "' result does not match the expected type: " +
			convErr.Message), nil
	}
	e.mu.Lock()
	if task.closed {
		e.mu.Unlock()
		return values.NewErrorWithMessage("Human task '" + id + "' is not running"), nil
	}
	task.result, task.completedBy = converted, args[3]
	task.completedAt = ctx.Env.Platform.Time.Now().UTC().Format(time.RFC3339Nano)
	task.closed = true
	close(task.done)
	e.mu.Unlock()
	return values.NewMap(e.anydataMapTy, e.anydataMapAt, false, []values.MapEntry{
		{Key: "taskName", Value: task.name}, {Key: "parentWorkflowId", Value: task.parent}}), nil
}

// lastHumanTaskCompletion answers the latest completed task of the workflow, by name or any.
func lastHumanTaskCompletion(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	inst := args[0].(*instance)
	name, byName := args[1].(string)
	for i := len(inst.completions) - 1; i >= 0; i-- {
		task := inst.completions[i]
		if !byName || task.name == name {
			return moduleError(ctx, "newHumanTaskCompletion", task.id, task.name, task.completedBy, task.completedAt), nil
		}
	}
	return nil, nil
}

// denial is the jBallerina rule: a caller with no roles and no id is an internal path and
// passes; otherwise exclusions apply first, then the audience must name a role or the user.
func (a taskAudience) denial(roles []string, rolesGiven bool, user string, userGiven bool, subject string) string {
	if !rolesGiven && !userGiven {
		return ""
	}
	if anyIn(roles, a.excludedRoles) {
		return "Unauthorized: caller holds a role excluded from " + subject
	}
	if len(a.excludedUsers) > 0 {
		if !userGiven {
			return "Unauthorized: " + subject + " excludes users and the caller has no user id"
		}
		if anyIn([]string{user}, a.excludedUsers) {
			return "Unauthorized: caller is excluded from " + subject
		}
	}
	if len(a.userRoles) == 0 && len(a.users) == 0 {
		return ""
	}
	if anyIn(roles, a.userRoles) || (userGiven && anyIn([]string{user}, a.users)) {
		return ""
	}
	if len(a.userRoles) == 0 && !userGiven {
		return "Unauthorized: " + subject + " is assigned to users and the caller has no user id"
	}
	users := ""
	if len(a.users) > 0 {
		users = fmt.Sprintf(" or user in %v", a.users)
	}
	return fmt.Sprintf("Unauthorized: caller does not have a required role to complete %s. Required one of: %v%s",
		subject, a.userRoles, users)
}

func anyIn(candidates, set []string) bool {
	for _, c := range candidates {
		for _, s := range set {
			if c == s {
				return true
			}
		}
	}
	return false
}

func field(m *values.Map, name string) values.BalValue {
	v, _ := m.Get(name)
	return v
}

// stringsOf reads a `string|[string, string...]?` field.
func stringsOf(v values.BalValue) []string {
	switch v := v.(type) {
	case string:
		return []string{v}
	case *values.List:
		out := make([]string, v.Len())
		for i := range out {
			out[i] = v.Get(i).(string)
		}
		return out
	}
	return nil
}

// isoDuration renders milliseconds the way java.time.Duration prints: PT24H, PT0.05S.
func isoDuration(millis int64) string {
	d := time.Duration(millis) * time.Millisecond
	hours, minutes := d/time.Hour, (d%time.Hour)/time.Minute
	seconds := (d % time.Minute).Seconds()
	var b strings.Builder
	b.WriteString("PT")
	if hours > 0 {
		fmt.Fprintf(&b, "%dH", int64(hours))
	}
	if minutes > 0 {
		fmt.Fprintf(&b, "%dM", int64(minutes))
	}
	if seconds > 0 || (hours == 0 && minutes == 0) {
		fmt.Fprintf(&b, "%gS", seconds)
	}
	return b.String()
}
