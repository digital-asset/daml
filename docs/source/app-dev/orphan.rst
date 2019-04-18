.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0


Information that doesn't have a home yet
#########################################

Transaction and transaction trees
*********************************

``TransactionService`` offers several different subscriptions. The most commonly used is the `GetTransactions` service. If you need more details, you can use `GetTransactionTrees` instead, which returns transactions as flattened trees, represented as a map of event IDs to events and a list of root event IDs.

DAML-LF
*******

DAML is compiled into a machine-readable format called DAML-LF ("DAML-Ledger Fragment"). The relationship between the surface DAML syntax and DAML-LF is similar to that between Java and JVM bytecode.

DAML-LF content appears in the package service interactions. It is represented as opaque blobs that require a secondary decoding phase.

Commonly used types
*******************

Primitive and structured types (records, variants and lists) appearing in the contract constructors and choice arguments are compatible with the types defined in the current version of DAML-LF (v1). They appear in the submitted commands and in the event streams.

There are some identifier fields that are represented as strings in the protobuf messages. They are opaque: you shouldn't interpret them in client code. They include:

-  Transaction IDs
-  Event IDs
-  Contract IDs
-  Package IDs (part of template identifiers)

There are some other identifiers that are determined by your client code. These aren't interpreted by the server, and are transparently passed to the responses. They include:

- Command IDs: used to uniquely identify a command and to match it against its response.
- Application ID: used to uniquely identify client process talking to the server. You could use a combination of command ID and application ID for deduplication.
-  Workflow IDs: identify chains of transactions. You can use these to correlate transactions sent across time spans and by different parties.

Versioning
**********

Ledger API uses standard protobuf versioning. At the moment, all the definitions are in the v1 namespace.

On the deployment level, software packages containing Ledger API are versioned using `semantic versioning <https://semver.org>`__. This enables first-glance discovery of any alterations compared to previous versions.
