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
	"time"

	"github.com/ballerina-nutcracker/ballerina/runtime/extern"
	"github.com/ballerina-nutcracker/ballerina/semtypes"
	"github.com/ballerina-nutcracker/ballerina/values"
)

// await waits until minCount of the futures complete, or the bound passes, and answers the
// completed values positionally in the caller's T: a tuple or array type gets its members
// converted, with nil at an incomplete position; anything else gets an anydata[].
func (e *engine) await(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	list := args[1].(*values.List)
	minCount := args[2].(int64)
	futures := make([]*values.Future, list.Len())
	unique := make([]*values.Future, 0, list.Len())
	seen := make(map[*values.Future]bool, list.Len())
	for i := range futures {
		f := list.Get(i).(*values.Future)
		futures[i] = f
		if !seen[f] {
			seen[f] = true
			unique = append(unique, f)
		}
	}
	if (minCount < 1 && len(unique) > 0) || minCount > int64(len(unique)) {
		return values.NewErrorWithMessage(fmt.Sprintf("Invalid minCount=%d for %d futures: minCount must be between 1 and %d",
			minCount, len(unique), len(unique))), nil
	}
	timeout, boundErr := durationMillis(args[3], "a timeout")
	if boundErr != nil {
		return boundErr, nil
	}
	clock := ctx.Env.Platform.Time
	deadline := clock.MonotonicNow() + time.Duration(timeout)*time.Millisecond
	for countComplete(unique) < int(minCount) {
		if timeout >= 0 && clock.MonotonicNow() >= deadline {
			return values.NewErrorWithMessage(fmt.Sprintf("Timeout waiting for futures: only %d of %d completed within the specified duration",
				countComplete(unique), minCount)), nil
		}
		<-ctx.Yield()
	}
	// A future is read once however often it is listed; the read claims it, as a `wait` would
	read := make(map[*values.Future]values.BalValue, len(unique))
	results := make([]values.BalValue, len(futures))
	completed := make([]bool, len(futures))
	for i, f := range futures {
		if !f.IsComplete() {
			continue
		}
		v, ok := read[f]
		if !ok {
			v = f.Get()
			read[f] = v
		}
		results[i], completed[i] = v, true
	}
	return e.shapeAwaitResult(ctx, results, completed, args[4].(*values.TypeDesc).Constraint()), nil
}

func countComplete(futures []*values.Future) int {
	n := 0
	for _, f := range futures {
		if f.IsComplete() {
			n++
		}
	}
	return n
}

func (e *engine) shapeAwaitResult(ctx *extern.Context, results []values.BalValue, completed []bool,
	target semtypes.SemType) values.BalValue {
	cx := ctx.TypeCtx()
	// T is inferred from the assignment, so it carries the `|error` (and `?`) of a caller that
	// did not `check`: the list shape is what is left once those are stripped
	shape := semtypes.Diff(target, semtypes.Union(semtypes.Error, semtypes.Nil))
	if semtypes.IsEmpty(cx, shape) || !semtypes.IsSubtype(cx, shape, semtypes.List) {
		return values.NewList(e.anydataListTy, e.anydataListAt, false, nil, 0, results)
	}
	atomic := semtypes.ToListAtomicType(ctx.TypeEnv(), shape)
	if atomic == nil {
		return values.NewList(e.anydataListTy, e.anydataListAt, false, nil, 0, results)
	}
	if fixed := atomic.FixedLength(); len(results) > fixed && semtypes.IsNever(atomic.Rest()) {
		return values.NewErrorWithMessage(fmt.Sprintf("await result has %d value(s) but the expected tuple type has only %d member(s)",
			len(results), fixed))
	}
	out := make([]values.BalValue, len(results))
	for i, v := range results {
		if !completed[i] {
			continue
		}
		member := atomic.MemberAtInnerVal(i)
		converted, convErr := values.CloneWithType(cx, v, member)
		if convErr != nil {
			// A position whose type admits an error carries its own; otherwise the wait fails
			if semtypes.IsEmpty(cx, semtypes.Intersect(member, semtypes.Error)) {
				return convErr
			}
			converted = convErr
		}
		out[i] = converted
	}
	return values.NewList(shape, atomic, false, nil, 0, out)
}
