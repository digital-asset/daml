.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

The Ledger API
##############

.. toctree::
   :hidden:

   services
   parties-users
   grpc/index
   grpc/error-codes
   grpc/proto-docs
   grpc/daml-to-ledger-api
   daml-lf-translation
   bindings-x-lang/index


To write an application around a Daml ledger, you will need to interact with the **Ledger API**.

Every ledger that Daml can run on exposes this same API.

What's in the Ledger API
************************

The Ledger API exposes the following services:

- Submitting commands to the ledger

  - Use the :ref:`command submission service <command-submission-service>` to submit commands (create a contract or exercise a choice) to the ledger.
  - Use the :ref:`command completion service <command-completion-service>` to track the status of submitted commands.
  - Use the :ref:`command service <command-service>` for a convenient service that combines the command submission and completion services.
- Reading from the ledger

  - Use the :ref:`transaction service <transaction-service>` to stream committed transactions and the resulting events (choices exercised, and contracts created or archived), and to look up transactions.
  - Use the :ref:`active contracts service <active-contract-service>` to quickly bootstrap an application with the currently active contracts. It saves you the work to process the ledger from the beginning to obtain its current state.
- Utility services

  - Use the :ref:`party management service <party-service>` to allocate and find information about parties on the Daml ledger.
  - Use the :ref:`package service <package-service>` to query the Daml packages deployed to the ledger.
  - Use the :ref:`ledger identity service <ledger-identity-service>` to retrieve the Ledger ID of the ledger the application is connected to.
  - Use the :ref:`ledger configuration service <ledger-configuration-service>` to retrieve some dynamic properties of the ledger, like maximum deduplication duration for commands.
  - Use the :ref:`version service <version-service>` to retrieve information about the Ledger API version.
  - Use the :ref:`user management service <user-management-service>` to manage users and their rights.
  - Use the :ref:`metering report service <metering-report-service>` to retrieve a participant metering report.
- Testing services (on Sandbox only, *not* for production ledgers)

  - Use the :ref:`time service <time-service>` to obtain the time as known by the ledger.

For full information on the services see :doc:`/app-dev/services`.

You may also want to read the :doc:`protobuf documentation </app-dev/grpc/proto-docs>`, which explains how each service is defined as protobuf messages.

How to Access the Ledger API
****************************

You can access the Ledger API via the :doc:`Java Bindings <bindings-java/index>` or the :doc:`Python Bindings </app-dev/bindings-python>` (formerly known as DAZL).

If you don't use a language that targets the JVM or Python, you can use gRPC to generate the code to access the Ledger API in
several supported programming languages. :doc:`Further documentation <bindings-x-lang/index>` provides a few
pointers on how you may want to approach this.

You can also use the :doc:`HTTP JSON API Service </json-api/index>` to tap into the Ledger API.

At its core, this service provides a simplified view of the active contract set and additional primitives to query it and
exposing it using a well-defined JSON-based encoding over a conventional HTTP connection.

A subset of the services mentioned above is also available as part of the HTTP JSON API.

.. _daml-lf-intro:

Daml-LF
*******

When you :ref:`compile Daml source into a .dar file <assistant-manual-building-dars>`, the underlying format is Daml-LF. Daml-LF is similar to Daml, but is stripped down to a core set of features. The relationship between the surface Daml syntax and Daml-LF is loosely similar to that between Java and JVM bytecode.

As a user, you don't need to interact with Daml-LF directly. But internally, it's used for:

- Executing Daml code on the Sandbox or on another platform
- Sending and receiving values via the Ledger API (using a protocol such as gRPC)
- Generating code in other languages for interacting with Daml models (often called “codegen”)

.. Daml-LF content appears in the package service interactions. It is represented as opaque blobs that require a secondary decoding phase.

When You Need to Know About Daml-LF
===================================

Daml-LF is only really relevant when you're dealing with the objects you send to or receive from the ledger. If you use any of the provided language bindings for the Ledger API, you don't need to know about Daml-LF at all, because this generates idiomatic representations of Daml for you.

Otherwise, it can be helpful to know what the types in your Daml code look like at the Daml-LF level, so you know what to expect from the Ledger API.

For example, if you are writing an application that creates some Daml contracts, you need to construct values to pass as parameters to the contract. These values are determined by the Daml-LF types in that contract template. This means you need an idea of how the Daml-LF types correspond to the types in the original Daml model.

For the most part the translation of types from Daml to Daml-LF should not be surprising. :doc:`This page goes through all the cases in detail </app-dev/daml-lf-translation>`.

For the bindings to your specific programming language, you should refer to the language-specific documentation.
