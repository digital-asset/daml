# Mock scala/codegen example
This example demonstrates how to:
- create a new contract from a template
- instantiate create and exercise ledger commands
```
$ sbt -Dda.sdk.version=100.12.12 mock-example/run
```

# Sandbox scala/codegen example
This examples demonstrates how to:
- start in-process sandbox
- create a new contract from a template
- send corresponding create an exercise commands to the ledger
- receive events from the ledger
- stop in-process sandbox
```
$ sbt -Dda.sdk.version=100.12.12 sandbox-example/run
```
