.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Writing applications using the Ledger API
#########################################

.. toctree::
   :hidden:

   services
   daml-lf-translation

DAML contracts are stored on a ledger. In order to exercise choices on those contracts, create new ones, or read from the ledger, you need to use the **Ledger API**. (Every ledger that DAML can run on exposes this same API.) And to do anything sophisticated, you'll want to write an application. 

Resources available to you
**************************

- **The Java bindings**: a library to help you write idiomatic applications using the Ledger API in Java.

  :doc:`Read the documentation for the Java bindings </app-dev/bindings-java/index>`
- **The experimental node.js bindings**: a library to help you write idiomatic applications using the Ledger API in JavaScript.

  :doc:`Read the documentation for the node.js </app-dev/bindings-js/index>`
- **The underlying gRPC API**: if you want to write an application for the ledger API in other languages, you'll need to use `gRPC <https://grpc.io>`__ directly.

  :doc:`Read the documentation for the gRPC API </app-dev/grpc/index>`
- **The application architecture guide**: this documentation gives high-level guidance on how to build your application.

  :doc:`Read the application architecture guide </app-dev/app-arch>`

What's in the Ledger API
************************

No matter how you're accessing it (Java bindings, node.js bindings, or gRPC), the Ledger API exposes the same services:

- Submitting commands to the ledger

  - Use the **command submission service** to submit commands (create a contract or exercise a choice) to the ledger.
  - Use the **command completion service** to track the status of submitted commands.
  - Use the **command service** for a convenient service that wraps both of the above.
- Reading from the ledger

  - Use the **transaction service** to retrieve transactions of events (contracts created and contracts archived) from the ledger.
  - Use the **active contract service** to quickly bootstrap an application with active contracts. It means you don't need to read from the beginning of the ledger or process create events for contracts that have already been archived.
- Utility services

  - Use the **package service** to query the DAML packages deployed to the ledger.
  - Use the **ledger identity service** to retrieve the Ledger ID of the ledger the application is connected to.
  - Use the **ledger configuration service** to retrieve some dynamic properties of the ledger, like minimum and maximum TTL for commands.
  - Use the **time service** to obtain the time as known by the ledger server.

For full information on the services see :doc:`/app-dev/services`.

You may also want to read the :doc:`protobuf documentation <proto-docs>`, which explains how each service is defined as protobuf messages.

.. _daml-lf-intro:

DAML-LF
*******

When you :ref:`compile DAML source into a .dar file <assistant-manual-building-dars>`, the underlying format is DAML-LF. DAML-LF is similar to DAML, but is stripped down to a core set of features. The relationship between the surface DAML syntax and DAML-LF is similar to that between Java and JVM bytecode.

As a user, you don't need to interact with DAML-LF directly. But inside the DAML SDK, it's used for:

- Executing DAML code on the Sandbox or on another platform
- Sending and receiving values via the Ledger API (using a protocol such as gRPC)
- Generating code in other languages for interacting with DAML models (often called “codegen”)

.. DAML-LF content appears in the package service interactions. It is represented as opaque blobs that require a secondary decoding phase.

When you need to know about DAML-LF
===================================

Knowledge of DAML-LF can be helpful when using the Ledger API or bindings on top of it. Development is easier if you know what the types in your DAML code look like at the DAML-LF level.

For example, if you are writing an application in Java that creates some DAML contracts, you need to construct values to pass as parameters to the contract. These values are determined by the Java classes generated from DAML-LF - specifically, by the DAML-LF types in that contract template. This means you need an idea of how the DAML-LF types correspond to the types in the original DAML model.

For the most part the translation of types from DAML to DAML-LF should not be surprising. :doc:`This page goes through all the cases in detail </app-dev/daml-lf-translation>`.

For the bindings to your specific programming language, you should refer to the language-specific documentation.
