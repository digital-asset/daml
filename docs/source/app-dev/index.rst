.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Writing applications using the Ledger API
#########################################

DAML contracts are stored on a ledger. In order to exercise choices on those contracts, create new ones, or read from the ledger, you need to use the **Ledger API**. (Every ledger that DAML can run on exposes this same API.) And to do anything sophisticated, you'll want to write an application. 

Resources available to you
**************************

- **The Java bindings**: a library to help you write idiomatic applications using the Ledger API in Java.
  - Introduction to the Java bindings
  - Getting started
  - Examples
  - Reference docs
  - Code generation
- **The experimental node.js bindings**: a library to help you write idiomatic applications using the Ledger API in JavaScript.
  - Introduction to the node.js bindings
  - Getting started
  - Examples
  - Reference
  - (no codegen yet)
- **The underlying gRPC API**: if you want to write an application for the ledger API in other languages, you'll need to use `gRPC <https://grpc.io>`__ directly.
  - Introduction to the gRPC API
  - Getting started
  - Examples
  - Protobuf reference

What's in the Ledger API
************************

No matter how you're accessing them (Java bindings, node.js bindings, or gRPC), the Ledger API exposes the same services:

- Submitting commands to the ledger

  - Use the **Command Submission Service** to submit commands (create a contract or exercise a choice) to the ledger.
  - Use the **Command Completion Service** to track the status of submitted commands.
  - Use the **Command Service** for a convenient service that wraps both of the above.
- Reading from the ledger

  - Use the **Transaction Service** to retrieve transactions of events (contracts created and contracts archived) from the ledger.
  - Use the **Active Contract Service** to quickly bootstrap an application with active contracts. It means you don't need to read from the beginning of the ledger or process create events for contracts that have already been archived.
- Utilities

  - Use the **Package Service** to query the DAML packages deployed to the ledger.
  - Use the **Ledger Identity Service** to retrieve the Ledger ID of the ledger the application is connected to.
  - Use the **Ledger Configuration Service** o retrieve some dynamic properties of the ledger, like minimum and maximum TTL for commands.

.. image:: ./images/services.svg

Transaction and transaction trees
=================================

``TransactionService`` offers several different subscriptions.
The most commonly used is the `GetTransactions` service. If you need more details, you can use `GetTransactionTrees` instead, which returns transactions as flattened trees, represented as a map of event IDs to events and a list of root event IDs.

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
