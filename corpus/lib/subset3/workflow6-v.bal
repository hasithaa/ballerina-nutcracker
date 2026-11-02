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

type Events record {|
    future<string> name;
    future<int> count;
|};

// Wait for every future and read the tuple
@workflow:Workflow
isolated function gather(workflow:Context ctx, string input, Events events) returns string|error {
    [string, int] both = check ctx->await([events.name, events.count]);
    return input + ": " + both[0] + "/" + both[1].toString();
}

// Wait for one of two: the other position is nil
@workflow:Workflow
isolated function firstOf(workflow:Context ctx, string input, Events events) returns string|error {
    [string?, int?] some = check ctx->await([events.name, events.count], minCount = 1);
    return input + ": " + some.toString();
}

// A bound that passes is an error value, as is a minCount the list cannot meet
@workflow:Workflow
isolated function bounded(workflow:Context ctx, string input, Events events) returns string|error {
    [string, int]|error late = ctx->await([events.name, events.count], timeout = {seconds: 0.01});
    [string]|error few = ctx->await([events.name], minCount = 2);
    string lateText = late is error ? late.message() : "?";
    string fewText = few is error ? few.message() : "?";
    return input + ": " + lateText + " | " + fewText;
}

// A tuple shorter than the list cannot hold the result
@workflow:Workflow
isolated function narrow(workflow:Context ctx, string input, Events events) returns string|error {
    [string]|error one = ctx->await([events.name, events.count]);
    return input + ": " + (one is error ? one.message() : one.toString());
}

public function main() returns error? {
    string g = check workflow:run(gather, "g");
    check workflow:sendData(gather, g, "name", "x");
    check workflow:sendData(gather, g, "count", 7);
    io:println(check workflow:waitForResult(g)); // @output g: x/7

    string f = check workflow:run(firstOf, "f");
    check workflow:sendData(firstOf, f, "name", "y");
    io:println(check workflow:waitForResult(f)); // @output f: ["y",null]

    io:println(check workflow:waitForResult(check workflow:run(bounded, "b"))); // @output b: Timeout waiting for futures: only 0 of 2 completed within the specified duration | Invalid minCount=2 for 1 futures: minCount must be between 1 and 1

    string n = check workflow:run(narrow, "n");
    check workflow:sendData(narrow, n, "name", "z");
    check workflow:sendData(narrow, n, "count", 1);
    io:println(check workflow:waitForResult(n)); // @output n: await result has 2 value(s) but the expected tuple type has only 1 member(s)
}
