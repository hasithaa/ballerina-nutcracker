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
	"time"

	"github.com/ballerina-nutcracker/ballerina/decimal"
	"github.com/ballerina-nutcracker/ballerina/runtime/extern"
	"github.com/ballerina-nutcracker/ballerina/values"
)

// retryPlan is an AutoRetry read off a callActivity's retryPolicy: attempts after the first,
// and the backoff between them in milliseconds.
type retryPlan struct {
	maxRetries   int64
	delayMillis  float64
	backoff      float64
	maxDelay     float64
	thenReviewed bool
}

// readRetryPolicy sorts a retryPolicy into no retries, an AutoRetry plan, or a review-bearing
// policy, which the in-memory engine cannot raise: nobody can decide a review without the
// management API. A mapping that spells out an audience field but names nobody is a mistake.
func readRetryPolicy(ctx *extern.Context, policy values.BalValue, activity string) (*retryPlan, values.BalValue) {
	m, ok := policy.(*values.Map)
	if !ok {
		return nil, nil
	}
	_, hasRoles := m.Get("userRoles")
	_, hasUsers := m.Get("users")
	reviewShaped := hasRoles || hasUsers
	namesAudience := len(stringsOf(field(m, "userRoles")))+len(stringsOf(field(m, "users"))) > 0
	_, hasRetries := m.Get("maxRetries")
	if reviewShaped && !namesAudience {
		return nil, values.NewErrorWithMessage("retryPolicy of '" + activity + "' must name 'userRoles' or 'users'")
	}
	if reviewShaped && !hasRetries {
		return nil, reviewUnsupported(ctx, activity)
	}
	plan := &retryPlan{maxRetries: 3, delayMillis: 1000, backoff: 2, thenReviewed: reviewShaped}
	if v, ok := field(m, "maxRetries").(int64); ok {
		plan.maxRetries = v
	}
	if v, ok := field(m, "retryDelay").(*decimal.Decimal); ok {
		plan.delayMillis = v.Float64() * 1000
	}
	if v, ok := field(m, "retryBackoff").(*decimal.Decimal); ok {
		plan.backoff = v.Float64()
	}
	if v, ok := field(m, "maxRetryDelay").(*decimal.Decimal); ok {
		plan.maxDelay = v.Float64() * 1000
	}
	return plan, nil
}

// readApprovalPolicy: NoApproval runs the call; a review gate cannot be raised in memory.
func readApprovalPolicy(ctx *extern.Context, policy values.BalValue, activity string) values.BalValue {
	m, ok := policy.(*values.Map)
	if !ok {
		return nil
	}
	if len(stringsOf(field(m, "userRoles")))+len(stringsOf(field(m, "users"))) == 0 {
		return values.NewErrorWithMessage("approvalPolicy of '" + activity + "' must name 'userRoles' or 'users'")
	}
	return reviewUnsupported(ctx, activity)
}

func reviewUnsupported(ctx *extern.Context, activity string) values.BalValue {
	return moduleError(ctx, "newReviewFailedError", "a review of '"+activity+
		"' cannot be raised by the in-memory engine: deciding one needs the management API")
}

// runWithRetries calls attempt until it answers a non-error or the plan is spent, sleeping
// the backoff between attempts while yielding. A spent RetryBeforeReview would raise a review.
func runWithRetries(ctx *extern.Context, plan *retryPlan, activity string,
	attempt func() values.BalValue) values.BalValue {
	result := attempt()
	if plan == nil {
		return result
	}
	delay := plan.delayMillis
	for retry := int64(0); retry < plan.maxRetries; retry++ {
		if _, failed := result.(*values.Error); !failed {
			return result
		}
		if plan.maxDelay > 0 && delay > plan.maxDelay {
			delay = plan.maxDelay
		}
		sleepYielding(ctx, time.Duration(delay*float64(time.Millisecond)))
		delay *= plan.backoff
		result = attempt()
	}
	if _, failed := result.(*values.Error); failed && plan.thenReviewed {
		return reviewUnsupported(ctx, activity)
	}
	return result
}

// sleepYielding waits on the PAL clock while yielding, as runtime:sleep does, so the strands
// sharing this thread keep running.
func sleepYielding(ctx *extern.Context, d time.Duration) {
	clock := ctx.Env.Platform.Time
	deadline := clock.MonotonicNow() + d
	for clock.MonotonicNow() < deadline {
		<-ctx.Yield()
	}
}
