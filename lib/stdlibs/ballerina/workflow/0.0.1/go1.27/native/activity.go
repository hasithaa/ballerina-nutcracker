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
	"strings"

	"github.com/ballerina-nutcracker/ballerina/runtime"
	"github.com/ballerina-nutcracker/ballerina/runtime/extern"
	"github.com/ballerina-nutcracker/ballerina/values"
)

const activityModuleName = "workflow.activity"

func initActivityModule(rt *runtime.Runtime) {
	runtime.RegisterExternFunction(rt, orgName, activityModuleName, "callRestAPI", callRestAPI)
}

// callRestAPI dispatches to the http:Client's remote method named by the RestMethod, with the
// activity's T as the client's targetType.
func callRestAPI(ctx *extern.Context, args []values.BalValue) (values.BalValue, error) {
	client := args[0].(*values.Object)
	method := strings.ToLower(args[1].(string))
	path, message, headers, td := args[2], args[3], args[4], args[5]
	handle, ok := ctx.LookupRemoteMethod(client, method)
	if !ok {
		return values.NewErrorWithMessage("Unsupported REST method: " + args[1].(string)), nil
	}
	var callArgs []values.BalValue
	if method == "get" {
		callArgs = []values.BalValue{client, path, headers, td}
	} else {
		callArgs = []values.BalValue{client, path, message, headers, nil, td}
	}
	result, err := ctx.InvokeMethod(handle, callArgs)
	if err != nil {
		return values.NewErrorWithMessage(err.Error()), nil
	}
	return result, nil
}

func init() {
	runtime.RegisterModuleInitializer(initActivityModule)
}
