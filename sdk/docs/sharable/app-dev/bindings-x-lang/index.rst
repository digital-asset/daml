.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Create Your Own Bindings
########################

This page gets you started with creating custom bindings for a Daml Ledger.

Bindings for a language consist of two main components:

- Ledger API
    Client "stubs" for the programming language, -- the remote API that allows sending ledger commands and receiving ledger transactions. You have to generate **Ledger API** from `the gRPC protobuf definitions in the daml repository on GitHub <https://github.com/digital-asset/daml/tree/main/ledger-api/grpc-definitions>`_. **Ledger API** is documented on this page: :doc:`/app-dev/grpc/index`. The `gRPC <https://grpc.io/docs/>`_ tutorial explains how to generate client "stubs".

- Codegen
     A code generator is a program that generates classes representing Daml contract templates in the language. These classes incorporate all boilerplate code for constructing: :ref:`com.daml.ledger.api.v1.CreateCommand` and :ref:`com.daml.ledger.api.v1.ExerciseCommand` corresponding for each Daml contract template.

Technically codegen is optional. You can construct the commands manually from the auto-generated **Ledger API** classes. However, it is very tedious and error-prone. If you are creating *ad hoc* bindings for a project with a few contract templates, writing a proper codegen may be overkill. On the other hand, if you have hundreds of contract templates in your project or are planning to build language bindings that you will share across multiple projects, we recommend including a codegen in your bindings. It will save you and your users time in the long run.

Note that for different reasons we chose codegen, but that is not the only option. There is really a broad category of metaprogramming features that can solve this problem just as well or even better than codegen; they are language-specific, but often much easier to maintain (i.e. no need to add a build step). Some examples are:

- `F# Type Providers <https://docs.microsoft.com/en-us/dotnet/fsharp/tutorials/type-providers/creating-a-type-provider#a-type-provider-that-is-backed-by-local-data>`_

- `Template Haskell <https://wiki.haskell.org/Template_Haskell>`_

Build Ledger Commands
=====================

No matter what approach you take, either manually building commands or writing a codegen to do this, you need to understand how ledger commands are structured. This section demonstrates how to build create and exercise commands manually and how it can be done using contract classes.

Create Command
--------------

Let's recall an **IOU** example from the :doc:`Quickstart guide </app-dev/bindings-java/quickstart>`, where `Iou` template is defined like this:

.. literalinclude:: /app-dev/bindings-java/quickstart/template-root/daml/Iou.daml
  :language: daml
  :start-after: -- BEGIN_IOU_TEMPLATE_DATATYPE
  :end-before: -- END_IOU_TEMPLATE_DATATYPE

If you do not specify any of the above fields or type their names or values incorrectly, or do not order them exactly as they are in the Daml template, the above code will compile but fail at run-time because you did not structure your create command correctly.

Exercise Command
----------------

To build :ref:`com.daml.ledger.api.v1.ExerciseCommand` for `Iou_Transfer`:

.. literalinclude:: /app-dev/bindings-java/quickstart/template-root/daml/Iou.daml
  :language: daml
  :start-after: -- BEGIN_IOU_TEMPLATE_TRANSFER
  :end-before: -- END_IOU_TEMPLATE_TRANSFER

Summary
=======

When creating custom bindings for Daml Ledgers, you will need to:

- generate **Ledger API** from the gRPC definitions

- decide whether to write a codegen to generate ledger commands or manually build them for all contracts defined in your Daml model.

The above examples should help you get started. If you are creating custom binding or have any questions, see the :doc:`/support/support` page for how to get in touch with us.

Links
=====

- gRPC documentation: https://grpc.io/docs/

- Documentation for Protobuf "well known types": https://developers.google.com/protocol-buffers/docs/reference/google.protobuf

- Daml Ledger API gRPC Protobuf definitions
    - current main: https://github.com/digital-asset/daml/tree/main/ledger-api/grpc-definitions
    - for specific versions: https://github.com/digital-asset/daml/releases

- Required gRPC Protobuf definitions:
    - https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/status/status.proto
    - https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/health/v1/health.proto
