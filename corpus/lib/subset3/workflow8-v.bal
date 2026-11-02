// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
//
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


import ballerina/io;
import ballerina/workflow;

isolated int attempts = 0;

// Fails until the attempt number reaches succeedOn
@workflow:Activity
isolated function flaky(int succeedOn) returns string|error {
    int n;
    lock {
        attempts += 1;
        n = attempts;
    }
    if n < succeedOn {
        return error("attempt " + n.toString() + " failed");
    }
    return "ok after " + n.toString();
}

isolated function resetAttempts() {
    lock {
        attempts = 0;
    }
}

isolated function attemptsMade() returns int {
    lock {
        return attempts;
    }
}

@workflow:Workflow
isolated function retried(workflow:Context ctx, int input) returns string|error {
    resetAttempts();
    string|error r = ctx->callActivity(flaky, {succeedOn: input},
            retryPolicy = {maxRetries: 3, retryDelay: 0.01, retryBackoff: 1.0});
    string text = r is error ? r.message() : r;
    return text + " in " + attemptsMade().toString() + " attempts";
}

@workflow:Workflow
isolated function once(workflow:Context ctx, int input) returns string|error {
    resetAttempts();
    string|error r = ctx->callActivity(flaky, {succeedOn: input});
    string text = r is error ? r.message() : r;
    return text + " in " + attemptsMade().toString() + " attempts";
}

@workflow:Workflow
isolated function reviewed(workflow:Context ctx, int input) returns string|error {
    resetAttempts();
    string|error gate = ctx->callActivity(flaky, {succeedOn: input}, approvalPolicy = {userRoles: "MANAGER"});
    string|error review = ctx->callActivity(flaky, {succeedOn: input}, retryPolicy = {userRoles: "MANAGER"});
    string|error spent = ctx->callActivity(flaky, {succeedOn: input},
            retryPolicy = {userRoles: "MANAGER", maxRetries: 1, retryDelay: 0.01});
    string|error nobody = ctx->callActivity(flaky, {succeedOn: input}, retryPolicy = {userRoles: ()});
    string kinds = (gate is workflow:ReviewFailedError).toString() + (review is workflow:ReviewFailedError).toString()
        + (spent is workflow:ReviewFailedError).toString();
    string spentText = spent is error ? spent.message() : spent;
    string nobodyText = nobody is error ? nobody.message() : nobody;
    return kinds + " | " + spentText + " | " + nobodyText + " | " + attemptsMade().toString() + " attempts";
}

public function main() returns error? {
    io:println(check workflow:waitForResult(check workflow:run(retried, 3))); // @output ok after 3 in 3 attempts
    io:println(check workflow:waitForResult(check workflow:run(retried, 9))); // @output attempt 4 failed in 4 attempts
    io:println(check workflow:waitForResult(check workflow:run(once, 2))); // @output attempt 1 failed in 1 attempts
    io:println(check workflow:waitForResult(check workflow:run(reviewed, 9))); // @output truetruetrue | a review of 'flaky' cannot be raised by the in-memory engine: deciding one needs the management API | retryPolicy of 'flaky' must name 'userRoles' or 'users' | 2 attempts
}
