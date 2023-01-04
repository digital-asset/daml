.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _grpc:

Use the Ledger API With gRPC
############################


If you want to write an application for the ledger API in other languages, you'll need to use `gRPC <https://grpc.io>`__ directly.

If you're not familiar with gRPC and protobuf, we strongly recommend following the `gRPC quickstart <https://grpc.io/docs/quickstart/>`__ and `gRPC tutorials <https://grpc.io/docs/tutorials/>`__. This documentation is written assuming you already have an understanding of gRPC.

Get Started
***********

You can get the protobufs from a `GitHub release <protobufs_>`_, or from the ``daml`` repository `here <https://github.com/digital-asset/daml/tree/main/ledger-api/grpc-definitions>`__.

Protobuf Reference Documentation
********************************

For full details of all of the Ledger API services and their RPC methods, see  :doc:`/app-dev/grpc/proto-docs`.

Example Project
***************

We have an example project demonstrating the use of the Ledger API with gRPC. To get the example project, ``PingPongGrpc``:

#. Configure your machine to use the example by following the instructions at :ref:`bindings-java-setup-maven`.
#. Clone the `repository from GitHub <https://github.com/digital-asset/ex-java-bindings>`__.
#. Follow the `setup instructions in the README <https://github.com/digital-asset/ex-java-bindings/blob/master/README.rst#setting-up-the-example-projects>`__. Use ``examples.pingpong.grpc.PingPongGrpcMain`` as the main class.

About the Example Project
=========================

The example shows very simply how two parties can interact via a ledger, using two Daml contract templates, ``Ping`` and ``Pong``.

The logic of the application goes like this:

#. The application injects a contract of type ``Ping`` for ``Alice``.
#. ``Alice`` sees this contract and exercises the consuming choice ``RespondPong`` to create a contract of type ``Pong`` for ``Bob``.
#. ``Bob`` sees this contract and exercises the consuming choice ``RespondPing``  to create a contract of type ``Ping`` for ``Alice``.
#. Points 2 and 3 are repeated until the maximum number of contracts defined in the Daml is reached.

The entry point for the Java code is the main class ``src/main/java/examples/pingpong/grpc/PingPongGrpcMain.java``. Look at it to see how connect to and interact with a ledger using gRPC.

The application prints output like this:

.. code-block:: text

    Bob is exercising RespondPong on #1:0 in workflow Ping-Alice-1 at count 0
    Alice is exercising RespondPing on #344:1 in workflow Ping-Alice-7 at count 9

The first line shows:

- ``Bob`` is exercising the ``RespondPong`` choice on the contract with ID ``#1:0`` for the workflow ``Ping-Alice-1``.
- Count ``0`` means that this is the first choice after the initial ``Ping`` contract.
- The workflow ID  ``Ping-Alice-1`` conveys that this is the workflow triggered by the second initial ``Ping`` contract that was created by ``Alice``.

This example subscribes to transactions for a single party, as different parties typically live on different participant nodes. However, if you have multiple parties registered on the same node, or are running an application against the Sandbox, you can subscribe to transactions for multiple parties in a single subscription by putting multiple entries into the ``filters_by_party`` field of the ``TransactionFilter`` message. Subscribing to transactions for an unknown party will result in an error.

Daml Types and Protobuf
***********************

For information on how Daml types and contracts are represented by the Ledger API as protobuf messages, see :doc:`/app-dev/grpc/daml-to-ledger-api`.

Error Handling
**************

The Ledger API generally uses the gRPC standard status codes for signaling response failures to client applications.

For more details on the gRPC standard status codes, see the `gRPC documentation <https://github.com/grpc/grpc/blob/600272c826b48420084c2ff76dfb0d34324ec296/doc/statuscodes.md>`__ .

Generically, on submitted commands the Ledger API responds with the following gRPC status codes:

ABORTED
   The platform failed to record the result of the command due to a transient server-side error (e.g. backpressure due to high load) or a time constraint violation. You can retry the submission. In case of a time constraint violation, please refer to the section :ref:`Dealing with time <dealing-with-time>` on how to handle commands with long processing times.
DEADLINE_EXCEEDED (when returned by the Command Service)
   The request might not have been processed, as its deadline expired before its completion was signalled.
ALREADY_EXISTS
   The command was rejected because the resource (e.g. contract key) already exists or because it was sent within the deduplication period of a previous command with the same change ID.
NOT_FOUND
   The command was rejected due to a missing resources (e.g. contract key not found).
INVALID_ARGUMENT
   The submission failed because of a client error. The platform will definitely reject resubmissions of the same command.
FAILED_PRECONDITION
   The command was rejected due to an interpretation error or due to a consistency error due to races.
OK (when returned by the Command Submission Service)
   Assume that the command was accepted and wait for the resulting completion or a timeout from the Command Completion Service.
OK (when returned by the Command Service)
   You can be sure that the command was successful.
INTERNAL, UNKNOWN (when returned by the Command Service)
   An internal system fault occurred. Contact the participant operator for the resolution.

Aside from the standard gRPC status codes, the failures returned by the Ledger API are enriched with details meant to help the application
or the application developer to handle the error autonomously (e.g. by retrying on a retryable error).
For more details on the rich error details see the :doc:`error-codes`
