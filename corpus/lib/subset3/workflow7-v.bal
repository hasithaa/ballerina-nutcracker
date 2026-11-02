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
import ballerina/lang.runtime;
import ballerina/workflow;

type Decision record {|
    boolean approved;
    string note;
|};

@workflow:Workflow
isolated function approval(workflow:Context ctx, string input) returns string|error {
    Decision d = check ctx->awaitHumanTask("approve", {request: input}, userRoles = "MANAGER", excludedUsers = "eve");
    workflow:HumanTaskCompletion? who = ctx.lastHumanTaskCompletion();
    string actor = who is () ? "nobody" : (who.completedBy ?: "anonymous");
    workflow:HumanTaskCompletion? other = ctx.lastHumanTaskCompletion("other");
    string others = other is () ? ", no other task" : ", other task?";
    return input + (d.approved ? " approved" : " rejected") + " (" + d.note + ") by " + actor + others;
}

@workflow:Workflow
isolated function timed(workflow:Context ctx, string input) returns string|error {
    Decision|workflow:HumanTaskError d = ctx->awaitHumanTask("approve", {}, userRoles = "MANAGER",
            timeout = {seconds: 0.01});
    if d is workflow:HumanTaskTimeoutError {
        return input + ": " + d.message();
    }
    return input;
}

@workflow:Workflow
isolated function nobody(workflow:Context ctx, string input) returns string|error {
    Decision|workflow:HumanTaskError d = ctx->awaitHumanTask("approve", {}, userRoles = ());
    return d is workflow:HumanTaskFailedError ? input + ": " + d.message() : input;
}

// The workflow strand creates the task after run returns, so a completion retries until it exists
function complete(string taskId, anydata result, [string, string...]? callerRoles = (), string? userId = ())
        returns error? {
    while true {
        error? outcome = workflow:completeHumanTask(taskId, result, callerRoles, userId);
        if outcome is () || !outcome.message().includes("not found") {
            return outcome;
        }
        runtime:sleep(0.01);
    }
}

function describe(error? outcome) returns string {
    return outcome is () ? "ok" : outcome.message();
}

public function main() returns error? {
    string a = check workflow:run(approval, "req-1");
    io:println(describe(complete("humantask-1", {approved: true, note: "ok"}, ["CLERK"], "bob"))); // @output Unauthorized: caller does not have a required role to complete task 'humantask-1'. Required one of: [MANAGER]
    io:println(describe(complete("humantask-1", {approved: true, note: "ok"}, ["MANAGER"], "eve"))); // @output Unauthorized: caller is excluded from task 'humantask-1'
    io:println(describe(complete("humantask-1", {approved: "yes"}, ["MANAGER"], "ann")).substring(0, 59)); // @output Human task 'humantask-1' result does not match the expected
    io:println(describe(complete("humantask-1", {approved: true, note: "ok"}, ["MANAGER"], "ann"))); // @output ok
    io:println(check workflow:waitForResult(a)); // @output req-1 approved (ok) by ann, no other task
    io:println(describe(complete("humantask-1", {approved: false, note: "late"}, ["MANAGER"], "ann"))); // @output Human task 'humantask-1' is not running

    io:println(check workflow:waitForResult(check workflow:run(timed, "t"))); // @output t: Human task 'approve' timed out after PT0.01S
    io:println(check workflow:waitForResult(check workflow:run(nobody, "n"))); // @output n: Human task 'approve' must name 'userRoles' or 'users'
    io:println(describe(workflow:completeHumanTask("no-such-task", {}))); // @output Human task 'no-such-task' not found
}
