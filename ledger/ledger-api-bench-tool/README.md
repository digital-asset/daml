# ledger-api-bench-tool

The `ledger-api-bench-tool` is a tool for measuring performance of a ledger.
It allows to run multiple concurrent streams reading transactions from a ledger and provides performance statistics
for such streams.

Please note that the `ledger-api-bench-tool` does not provide a load source for the ledger.

## Running
Run using `bazel run`:
```
bazel run -- //ledger/ledger-api-bench-tool --help
```
or using a fat jar:
```
bazel build //ledger/ledger-api-bench-tool:ledger-api-bench-tool_deploy.jar
java -jar bazel-bin/ledger/ledger-api-bench-tool/ledger-api-bench-tool_deploy.jar --help
```
