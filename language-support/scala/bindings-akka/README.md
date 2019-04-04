# Ledger Client Binding

This module contains the glue code between `nanobot-framework`, `scala-codegen`
and `prototype-client` (Scala binding for the ledger API). The gRPC API provided
by `prototype-client` as it is is not type-safe. The incomming `Transaction`s
from `LedgerClient` is routed to `DomainTransactionMapper`, which will:
* convert the data in `Transaction` to type-safe types coming from `api-refinements`
* call the `EventDecoder` provided by `scala-codegen` for created events
* verify that the messages contain all the neccessary fields, and remove `Optional` wrappers

The result of this will be the `Domain*` classes.


In the other directions (for the commands coming out from the nanobots) the
'CompositeCommandAdapter' will be used to transform back to the gRPC interface
(`SubmitRequest`). After the command submission, the outcoming `Completion` will
be used to check the result, and based on that the `CommandRetryFlow` can decide
wether the command should be retried or an error should be reported.
