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


import ballerina/http;
import ballerina/io;
import ballerina/workflow;
import ballerina/workflow.activity;

final http:Client backend = check new ("https://example.com");

@workflow:Workflow
isolated function fetch(workflow:Context ctx, string input) returns string|error {
    string text = check ctx->callActivity(activity:callRestAPI,
            {connection: backend, method: activity:GET, path: "/" + input, message: (), headers: ()});
    byte[] bytes = check ctx->callActivity(activity:callRestAPI,
            {connection: backend, method: activity:POST, path: "/" + input, message: {id: 1}, headers: {"X-Run": input}});
    json|error notJson = ctx->callActivity(activity:callRestAPI,
            {connection: backend, method: activity:DELETE, path: "/" + input, message: (), headers: ()});
    return text + " " + bytes.length().toString() + " " + (notJson is error).toString();
}

public function main() returns error? {
    io:println(check workflow:waitForResult(check workflow:run(fetch, "orders"))); // @output test body 9 true
}
