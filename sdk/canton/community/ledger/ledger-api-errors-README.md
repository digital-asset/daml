# Ledger error definitions

Home to error definitions commonly reported via the Ledger API server.
As opposed to definitions in `//ledger/participant-state-kv-errors`, these errors
are generic wrt to the ledger backend used by the participant server.

## Daml-LF dependencies

Multiple error definitions depend on Daml LF types whose Bazel targets
pull in unrelated dependencies, such as the Daml engine.

TODO(i12295): **TODO (https://github.com/digital-asset/daml/issues/15453):** Extract Daml-LF interface types to separate package
in order to decouple the error definitions from the Daml engine.
