.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

The Ledger API
##############

.. toctree::
   :hidden:

   services
   grpc/index
   grpc/proto-docs
   grpc/daml-to-ledger-api
   daml-lf-translation
   bindings-java/index
   bindings-scala/index
   bindings-js
   bindings-x-lang/index


To write an application around a DAML ledger, you'll need to interact with the **Ledger API** from
another language. Every ledger that DAML can run on exposes this same API.

What's in the Ledger API
************************

You can access the Ledger API via via the HTTP JSON API, Java bindings, Scala bindings or gRPC. In
all cases, the Ledger API exposes the same services:

- Submitting commands to the ledger

  - Use the :ref:`command submission service <command-submission-service>` to submit commands (create a contract or exercise a choice) to the ledger.
  - Use the :ref:`command completion service <command-completion-service>` to track the status of submitted commands.
  - Use the :ref:`command service <command-service>` for a convenient service that combines the command submission and completion services.
- Reading from the ledger

  - Use the :ref:`transaction service <transaction-service>` to stream committed transactions and the resulting events (choices exercised, and contracts created or archived), and to look up transactions.
  - Use the :ref:`active contracts service <active-contract-service>` to quickly bootstrap an application with the currently active contracts. It saves you the work to process the ledger from the beginning to obtain its current state.
- Utility services

  - Use the :ref:`package service <package-service>` to query the DAML packages deployed to the ledger.
  - Use the :ref:`ledger identity service <ledger-identity-service>` to retrieve the Ledger ID of the ledger the application is connected to.
  - Use the :ref:`ledger configuration service <ledger-configuration-service>` to retrieve some dynamic properties of the ledger, like maximum deduplication time for commands.
- Testing services (on Sandbox only, *not* for production ledgers)

  - Use the :ref:`time service <time-service>` to obtain the time as known by the ledger.
  - Use the :ref:`reset service <reset-service>` to reset the ledger state, as a quicker alternative to restarting the whole ledger application.

For full information on the services see :doc:`/app-dev/services`.

You may also want to read the :doc:`protobuf documentation </app-dev/grpc/proto-docs>`, which explains how each service is defined as protobuf messages.

.. _daml-lf-intro:

DAML-LF
*******

When you :ref:`compile DAML source into a .dar file <assistant-manual-building-dars>`, the underlying format is DAML-LF. DAML-LF is similar to DAML, but is stripped down to a core set of features. The relationship between the surface DAML syntax and DAML-LF is loosely similar to that between Java and JVM bytecode.

As a user, you don't need to interact with DAML-LF directly. But inside the DAML SDK, it's used for:

- Executing DAML code on the Sandbox or on another platform
- Sending and receiving values via the Ledger API (using a protocol such as gRPC)
- Generating code in other languages for interacting with DAML models (often called “codegen”)

.. DAML-LF content appears in the package service interactions. It is represented as opaque blobs that require a secondary decoding phase.

When you need to know about DAML-LF
===================================

DAML-LF is only really relevant when you're dealing with the objects you send to or receive from the ledger. If you use any of the provided language bindings for the Ledger API, you don't need to know about DAML-LF at all, because this generates idiomatic representations of DAML for you.

Otherwise, it can be helpful to know what the types in your DAML code look like at the DAML-LF level, so you know what to expect from the Ledger API.

For example, if you are writing an application that creates some DAML contracts, you need to construct values to pass as parameters to the contract. These values are determined by the DAML-LF types in that contract template. This means you need an idea of how the DAML-LF types correspond to the types in the original DAML model.

For the most part the translation of types from DAML to DAML-LF should not be surprising. :doc:`This page goes through all the cases in detail </app-dev/daml-lf-translation>`.

For the bindings to your specific programming language, you should refer to the language-specific documentation.
