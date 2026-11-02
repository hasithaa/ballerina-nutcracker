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

@workflow:Activity
isolated function double(int n) returns int {
    return n * 2;
}

@workflow:Workflow
isolated function child(workflow:Context ctx, int input) returns int|error {
    return ctx->callActivity(double, {n: input});
}

@workflow:Workflow
isolated function gated(workflow:Context ctx, string input, record {|future<string> go;|} events)
        returns string|error {
    string who = check wait events.go;
    string kind = check ctx.getWorkflowType();
    return input + " by " + who + " in " + kind;
}

@workflow:Workflow
isolated function failing(workflow:Context ctx, int input) returns int|error {
    string id = check ctx.getWorkflowId();
    return error("child failed " + input.toString() + " as " + id);
}

// Fan out two children, gather both, then call a third and wait in one step
@workflow:Workflow
isolated function fanOut(workflow:Context ctx, int input) returns int|error {
    string a = check ctx->runChildWorkflow(child, input);
    string b = check ctx->runChildWorkflow(child, input + 1);
    int x = check ctx->waitForChildWorkflow(a);
    int y = check ctx->waitForChildWorkflow(b);
    int z = check ctx->callWorkflow(child, 10);
    return x + y + z;
}

// The child waits on an event only its parent sends, so the early read is deterministic
@workflow:Workflow
isolated function coordinator(workflow:Context ctx, string input) returns string|error {
    string id = check ctx->runChildWorkflow(gated, input);
    string|error early = ctx->getChildWorkflowResult(id);
    boolean inProgress = early is workflow:WorkflowInProgressError;
    check ctx->sendDataToChildWorkflow(id, "go", "ann");
    string done = check ctx->waitForChildWorkflow(id);
    return done + (inProgress ? " (was in progress)" : "");
}

@workflow:Workflow
isolated function unlucky(workflow:Context ctx, int input) returns int|error {
    int|error r = ctx->callWorkflow(failing, input);
    if r is workflow:InstanceFailedError {
        return -1;
    }
    return r;
}

@workflow:Workflow
isolated function lost(workflow:Context ctx, int input) returns int|error {
    int|error r = ctx->waitForChildWorkflow("no-such-child");
    if r is workflow:InstanceNotFoundError {
        return input - 2;
    }
    return r;
}

public function main() returns error? {
    io:println(check workflow:waitForResult(check workflow:run(fanOut, 1))); // @output 26
    io:println(check workflow:waitForResult(check workflow:run(coordinator, "job"))); // @output job by ann in gated (was in progress)
    io:println(check workflow:waitForResult(check workflow:run(unlucky, 3))); // @output -1
    io:println(check workflow:waitForResult(check workflow:run(lost, 0))); // @output -2
    // A child is an instance of its own, readable from outside by its id
    io:println(check workflow:getStatus("child-2")); // @output COMPLETED
}
