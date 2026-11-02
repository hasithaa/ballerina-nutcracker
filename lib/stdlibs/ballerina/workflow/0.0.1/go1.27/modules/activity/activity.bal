// Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/http;

// The jBallerina source marks the function `@workflow:Activity` and infers `t` from the
// assignment. A bala's submodules get no intra-package import edges from the loader, so this
// module cannot import its root yet, and a dependently typed function cannot be passed as a
// value; the annotation is a marker only, and `callActivity` binds any function.

# HTTP methods `callRestAPI` can send.
public enum RestMethod {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH
}

# Calls a REST endpoint through a module-level `http:Client`, as an activity.
#
# + connection - The client, declared as a module-level `final` variable
# + method - The HTTP method
# + path - The resource path, appended to the client's URL
# + message - The payload for `POST`, `PUT`, `PATCH` and `DELETE`; ignored by `GET`
# + headers - Request headers
# + t - The expected response type. Left unnamed in a `callActivity`, it takes the call's `T`
# + return - The response bound to `t`, or an error
public isolated function callRestAPI(http:Client connection, RestMethod method,
        string path = "",
        http:Request|map<json>|json|xml|string|byte[]? message = (),
        map<string|string[]>? headers = (),
        typedesc<anydata> t = anydata) returns anydata|error = external;
