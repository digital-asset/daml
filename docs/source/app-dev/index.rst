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
- **The application architecture guide**: this documentation gives high-level guidance on how to build your application.

What's in the Ledger API
************************

No matter how you're accessing it (Java bindings, node.js bindings, or gRPC), the Ledger API exposes the same services:

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

For more information, see services.rst.

