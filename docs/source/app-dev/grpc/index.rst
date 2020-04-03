.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _grpc:

gRPC
####

If you want to write an application for the ledger API in other languages, you'll need to use `gRPC <https://grpc.io>`__ directly.

If you're not familiar with gRPC and protobuf, we strongly recommend following the `gRPC quickstart <https://grpc.io/docs/quickstart/>`__ and `gRPC tutorials <https://grpc.io/docs/tutorials/>`__. This documentation is written assuming you already have an understanding of gRPC.

Getting started
***************

You can get the protobufs from a :github-asset:`GitHub release<protobufs>`, or from the ``daml`` repository `here <https://github.com/digital-asset/daml/tree/master/ledger-api/grpc-definitions>`__.

Protobuf reference documentation
********************************

For full details of all of the Ledger API services and their RPC methods, see  :doc:`/app-dev/grpc/proto-docs`.

Example project
***************

We have an example project demonstrating the use of the Ledger API with gRPC. To get the example project, ``PingPongGrpc``:

#. Configure your machine to use the example by following the instructions at :ref:`bindings-java-setup-maven`.
#. Clone the `repository from GitHub <https://github.com/digital-asset/ex-java-bindings>`__. 
#. Follow the `setup instructions in the README <https://github.com/digital-asset/ex-java-bindings/blob/master/README.rst#setting-up-the-example-projects>`__. Use ``examples.pingpong.grpc.PingPongGrpcMain`` as the main class.

About the example project
=========================

The example shows very simply how two parties can interact via a ledger, using two DAML contract templates, ``Ping`` and ``Pong``.

The logic of the application goes like this:

#. The application injects a contract of type ``Ping`` for ``Alice``.
#. ``Alice`` sees this contract and exercises the consuming choice ``RespondPong`` to create a contract of type ``Pong`` for ``Bob``.
#. ``Bob`` sees this contract and exercises the consuming choice ``RespondPing``  to create a contract of type ``Ping`` for ``Alice``.
#. Points 2 and 3 are repeated until the maximum number of contracts defined in the DAML is reached.

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

DAML types and protobuf
***********************

For information on how DAML types and contracts are represented by the Ledger API as protobuf messages, see :doc:`/app-dev/grpc/daml-to-ledger-api`.

Error handling
**************

Tor the standard error codes that the server or the client might return, see the `gRPC documentation <https://github.com/grpc/grpc/blob/600272c826b48420084c2ff76dfb0d34324ec296/doc/statuscodes.md>`__ .

For submitted commands, there are these response codes:

ABORTED
   The platform failed to record the result of the command due to a transient server-side error or a time constraint violation. You can retry the submission. In case of a time constraint violation, please refer to the section :ref:`Dealing with time <dealing-with-time>` on how to handle commands with long processing times.
INVALID_ARGUMENT
   The submission failed because of a client error. The platform will definitely reject resubmissions of the same command.
OK, INTERNAL, UNKNOWN (when returned by the Command Submission Service)
   Assume that the command was accepted, and wait for the resulting completion or a timeout from the Command Completion Service.
OK (when returned by the Command Service)
   You can be sure that the command was successful.
INTERNAL, UNKNOWN (when returned by the Command Service)
   Resubmit the command with the same command_id.
