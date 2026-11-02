# Ballerina Workflow Library

## Overview

The `ballerina/workflow` library runs long-running business processes as workflow functions: a `@workflow:Workflow` function calls `@workflow:Activity` functions through its `Context`, waits for data sent in from outside through an events record of futures, sleeps, and is read back by id with `getResult`, `waitForResult` and `getStatus`.

The Go-native version runs every instance in process with an in-memory engine: a workflow is a strand, an activity is a direct call on that strand, and `sendData` completes the future of the named event. Nothing is persisted and nothing replays, so an instance does not survive the process. Running against a Temporal server is tracked as ballerina-nutcracker/ballerina#1479.

## Key Functionalities

- Declare workflows and activities (`@Workflow`, `@Activity`)
- Start an instance and get its id, or start it under an id of your own with a policy for a held id (`run`, `runWithId`)
- Send data to a running instance's events record (`sendData`)
- Read a result without waiting, wait for it with an optional bound, or ask the status (`getResult`, `waitForResult`, `getStatus`)
- Call activities with named arguments (`Context.callActivity`)
- Wait for some or all of the events record's futures with an optional bound (`Context.await`)
- Wait for a person's decision and complete it from outside (`Context.awaitHumanTask`, `completeHumanTask`, `Context.lastHumanTaskCompletion`)
- Sleep inside a workflow, read the clock, the instance id and type (`Context.sleep`, `currentTimeMillis`, `getWorkflowId`, `getWorkflowType`)
- Start child workflows, gather their results and send them data (`Context.runChildWorkflow`, `waitForChildWorkflow`, `getChildWorkflowResult`, `callWorkflow`, `sendDataToChildWorkflow`)
- Typed read errors (`WorkflowInProgressError`, `InstanceNotFoundError`, `InstanceFailedError`, `InstanceAlreadyExistsError`)

## Examples

```ballerina
import ballerina/io;
import ballerina/workflow;

type Approval record {|
    boolean ok;
    string who;
|};

@workflow:Activity
isolated function charge(int amount, int fee) returns int|error {
    return amount + fee;
}

@workflow:Workflow
isolated function processOrder(workflow:Context ctx, int input, record {|future<Approval> approval;|} events)
        returns string|error {
    int total = check ctx->callActivity(charge, {amount: input, fee: 5});
    Approval approval = check wait events.approval;
    return approval.ok ? "charged " + total.toString() + " by " + approval.who : "declined";
}

public function main() returns error? {
    string id = check workflow:run(processOrder, 10);
    io:println(workflow:getResult(id) is workflow:WorkflowInProgressError); // true
    check workflow:sendData(processOrder, id, "approval", {ok: true, who: "ann"});
    io:println(check workflow:waitForResult(id, timeout = {seconds: 5})); // charged 15 by ann
}
```

## Go Native Interpreter Support Status

This library is currently being migrated to Go to support the Ballerina Native Interpreter. The table below outlines the current support level for various features of this library in the Go implementation.

Support Levels:

- **Supported**: Fully implemented and tested in the Go version.
- **Partially Supported**: Implemented but lacking some edge cases, options, or sub-features. (See comments).
- **Not Yet Supported**: Planned for migration, but not yet implemented.
- **Cannot Support**: Cannot be implemented in the Go version due to technical limitations or architectural differences. (See comments).

| Feature/API | Support Status | Comments / Limitations |
|---|---|---|
| `@Workflow` and `@Activity` annotations | Partially Supported | Declared, but the compile-time checks of the jBallerina compiler plugin (a `@Workflow` function behind `run`, activity signatures, step ids) are not performed yet; tracked on ballerina-nutcracker/ballerina#1477 |
| `run` | Supported | Instance ids are `<workflowType>-<n>` |
| `sendData` | Supported | The payload is converted to the future's type; a value that does not convert is refused |
| `getResult` | Supported | |
| `waitForResult` | Supported | A negative bound and a bound with `months` or `years` are refused with an error |
| `getStatus` | Supported | Answers `RUNNING`, `COMPLETED` or `FAILED`; `SUSPENDED`, `CANCELED`, `TERMINATED` and `TIMED_OUT` need a durable engine |
| Read return types | Partially Supported | `getResult` and `waitForResult` return `T\|error`, not `T\|WorkflowInProgressError\|InstanceFailedError\|InstanceNotFoundError\|error`: the compiler rejects a dependent return whose dependent and defined parts overlap. The values are the typed errors, so `is` checks work |
| `WorkflowInProgressError`, `InstanceNotFoundError`, `InstanceFailedError` | Supported | Built through Ballerina factories the engine calls; `InstanceFailedError` carries the workflow's error as its cause with `status = FAILED` |
| `Context.callActivity` | Partially Supported | Every parameter must be named: a declared default is not applied, since the runtime does not expose default-value providers to externs. A surplus or missing name is an error. An activity panic comes back as an error |
| `Context.sleep` | Supported | Waits on the PAL clock while yielding, like `runtime:sleep`; `months` and `years` are refused |
| `Context.currentTimeMillis` | Supported | `pal.Time.Now` |
| `Context.isReplaying` | Supported | Always `false`: nothing replays in process |
| `Context.getWorkflowId`, `Context.getWorkflowType` | Supported | |
| `Context.await` | Supported | A read claims the future, as a `wait` does, so a later `wait` on a future that `await` answered reports a double wait |
| `Context.awaitHumanTask`, `completeHumanTask`, `lastHumanTaskCompletion` | Partially Supported | A task is held in memory under the id `humantask-<n>` and completed with `completeHumanTask`, which applies `userRoles`, `users`, `excludedUsers` and `excludedRoles` and the result type the workflow expects. No rejection, deadline move or reassignment: those are management operations. `administratorRoles`/`administratorUsers`, `title` and `description` are accepted and unused. A task is not listed anywhere: the management API is not ported |
| `Context.lastReviewDecision` | Not Yet Supported | Answers `()`: reviews come with the activity approval and retry policies |
| Child workflows (`runChildWorkflow`, `getChildWorkflowResult`, `waitForChildWorkflow`, `callWorkflow`, `sendDataToChildWorkflow`) | Supported | A child is an instance of its own, started from the parent's strand and readable from outside by its id; it is not cancelled when the parent closes |
| `runWithId` and the instance-id policies | Supported | A closed holder stays in memory for as long as the process, so `REJECT_DUPLICATE` refuses for that long |
| Durable agents (`DurableAgent`, `getPendingAgentEvents`) | Not Yet Supported | The `agent` source and the `ballerina/ai` dependency are not ported |
| Management API (`workflow.management`, `workflow.management.rest`) and `workflow.observe` | Not Yet Supported | Submodules are not ported |
| Durability and replay (Temporal server, `Mode` configuration) | Not Yet Supported | `mode` is a `final` `IN_MEMORY` because `configurable` is not supported (ballerina-nutcracker/ballerina#1478); a local Temporal server is ballerina-nutcracker/ballerina#1479 |
| Deprecated APIs (`getResultWithTimeout`, `awaitForResult`, …) | Cannot Support | Removed rather than ported: `@deprecated` is not supported and the module has their replacements |

### Notable Behavioural Changes

- **Instances are not durable.** jBallerina runs every instance on a Temporal worker, replaying its history after a crash; the Go-native version keeps instances in process memory, so they end with the process and `isReplaying` is always `false`.
- **An activity runs on the workflow's strand.** jBallerina schedules activities on worker threads with retries and timeouts; here `callActivity` is a direct call, and a panic in the activity comes back to the workflow as an error, as a failed activity does.
- **A child outlives its parent.** jBallerina cancels in-flight children when the parent closes; the in-memory engine has no cancellation, so a child keeps running to its own end and stays readable by its id.
- **A human task is found by its id only.** jBallerina lists pending tasks through the management API and a person answers one from an inbox; here the id is `humantask-<n>` in creation order and nothing lists the tasks, so the program completing a task must know its id.
- **A workflow panic fails the instance.** The panic value becomes the instance's error, so `waitForResult` answers an `InstanceFailedError` whose message is the panic's.
