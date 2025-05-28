.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _build_explanations_ledger_api_overview:

Ledger API overview
###################

What is the Ledger API
**********************

The **Ledger API** is an API that's exposed by any Participant Node. Users access and manipulate the ledger state through the Ledger API.
There are two protocols available for the Ledger API: gRPC and JSON. The core services are implemented using `gRPC <https://grpc.io/>`__
and `Protobuf <https://developers.google.com/protocol-buffers/>`__. There is a translation layer that additionally exposes all the
Ledger API services as :subsiteref:`JSON Ledger API <json-api>`.

For a high level introduction to the services see :ref:`ledger-api-services`.

You may also want to read :subsiteref:`the protobuf documentation of the API <build-reference-lapi-proto-docs>`, which explains how each service is defined through its protobuf messages.

.. _how-to-access-ledger-api:

How to Access the Ledger API
****************************

You can access the gRPC Ledger API via the :ref:`Java bindings <component-howtos-application-development-java-client-libraries-bindings>`.

If you don't use a language that targets the JVM, you can use gRPC to generate the code to access the Ledger API in
several programming languages that support the `proto` standard.

If you don't want to use the gRPC API, you can also use the :subsiteref:`JSON Ledger API <json-api>`. This API is formally
described using an :subsiteref:`openapi<reference-json-api-openapi>` and :subsiteref:`asynchapi<reference-json-api-asynchapi>`
descriptions. You can use any language that supports OpenAPI standard to generate the clients using the
`OpenAPI Genetors<https://openapi-generator.tech>`__.

.. _daml-lf-intro:

Daml-LF
*******

When you :ref:`compile Daml source into a .dar file <build_howto_build_dar_files>`, the underlying format is Daml-LF. Daml-LF is similar to Daml, but is stripped down to a core set of features. The relationship between the surface Daml syntax and Daml-LF is loosely similar to that between Java and JVM bytecode.

As a user, you don't need to interact with Daml-LF directly. But internally, it's used for:

- Executing Daml code on the Sandbox or on another platform
- Sending and receiving values via the Ledger API
- Generating code in other languages for interacting with Daml models (often called “codegen”)

Daml assistant offers two code generators (for Java and Typescript) that convert the DAML-LF to the generates idiomatic representations of Daml for you.

When You Need to Know About Daml-LF
===================================

Daml-LF is only really relevant when you're dealing with the objects you send to or receive from the ledger. If you use any of the provided code generators, you don't need to know about Daml-LF at all.

Otherwise, it can be helpful to know what the types in your Daml code look like at the Daml-LF level, so you know what to expect from the Ledger API.

For example, if you are writing an application that creates some Daml contracts, you need to construct values to pass as parameters to the contract. These values are determined by the Daml-LF types in that contract template. This means you need an idea of how the Daml-LF types correspond to the types in the original Daml model.

For the most part the translation of types from Daml to Daml-LF should not be surprising. :ref:`This page goes through all the cases in detail <daml-lf-translation>`.
