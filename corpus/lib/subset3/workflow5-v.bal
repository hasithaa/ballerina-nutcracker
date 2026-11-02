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

@workflow:Workflow
isolated function purchase(workflow:Context ctx, string input, record {|future<string> approval;|} events)
        returns string|error {
    string id = check ctx.getWorkflowId();
    string who = check wait events.approval;
    return id + ": " + input + " approved by " + who;
}

@workflow:Workflow
isolated function failing(workflow:Context ctx, string input) returns string|error {
    string id = check ctx.getWorkflowId();
    return error(id + " failed on " + input);
}

function describe(string|error outcome) returns string {
    if outcome is workflow:InstanceAlreadyExistsError {
        return "refused: " + outcome.message();
    }
    return outcome is error ? "error: " + outcome.message() : outcome;
}

public function main() returns error? {
    io:println(check workflow:runWithId(purchase, "order-1", "first")); // @output order-1
    // A running holder refuses by default and is joined with USE_EXISTING; the new input is dropped
    io:println(describe(workflow:runWithId(purchase, "order-1", "second"))); // @output refused: An instance with id 'order-1' already exists (status: RUNNING)
    io:println(describe(workflow:runWithId(purchase, "order-1", "second", ifRunning = workflow:USE_EXISTING))); // @output order-1
    check workflow:sendData(purchase, "order-1", "approval", "ann");
    io:println(check workflow:waitForResult("order-1")); // @output order-1: first approved by ann

    // A completed holder is replaced by default, kept with REJECT_DUPLICATE or ALLOW_DUPLICATE_FAILED_ONLY
    io:println(describe(workflow:runWithId(purchase, "order-1", "third", ifClosed = workflow:REJECT_DUPLICATE))); // @output refused: An instance with id 'order-1' already exists (status: COMPLETED)
    io:println(describe(workflow:runWithId(purchase, "order-1", "third", ifClosed = workflow:ALLOW_DUPLICATE_FAILED_ONLY))); // @output refused: An instance with id 'order-1' already exists (status: COMPLETED)
    io:println(check workflow:runWithId(purchase, "order-1", "third")); // @output order-1
    io:println(check workflow:getStatus("order-1")); // @output RUNNING
    check workflow:sendData(purchase, "order-1", "approval", "bob");
    io:println(check workflow:waitForResult("order-1")); // @output order-1: third approved by bob

    // A failed holder is replaced under ALLOW_DUPLICATE_FAILED_ONLY
    io:println(check workflow:runWithId(failing, "job-1", "x"));  // @output job-1
    string|error failed = workflow:waitForResult("job-1");
    io:println(describe(failed)); // @output error: job-1 failed on x
    io:println(check workflow:runWithId(failing, "job-1", "y", ifClosed = workflow:ALLOW_DUPLICATE_FAILED_ONLY)); // @output job-1
    io:println(describe(workflow:waitForResult("job-1"))); // @output error: job-1 failed on y
    string|error refused = workflow:runWithId(failing, "job-1", "z", ifClosed = workflow:REJECT_DUPLICATE);
    io:println(refused is workflow:InstanceAlreadyExistsError); // @output true
}
