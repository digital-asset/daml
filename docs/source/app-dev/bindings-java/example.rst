.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Java Bindings Example Project
#############################

To try out the Java bindings library, use the `examples on GitHub <https://github.com/digital-asset/ex-java-bindings>`__: ``PingPongReactive``.

The example implements the ``PingPong`` application, which consists of:

- a Daml model with two contract templates, ``Ping`` and ``Pong``
- two parties, ``Alice`` and ``Bob``

The logic of the application goes like this:

#. The application injects a contract of type ``Ping`` for ``Alice``.
#. ``Alice`` sees this contract and exercises the consuming choice ``RespondPong`` to create a contract of type ``Pong`` for ``Bob``.
#. ``Bob`` sees this contract and exercises the consuming choice ``RespondPing``  to create a contract of type ``Ping`` for ``Alice``.
#. Points 2 and 3 are repeated until the maximum number of contracts defined in the Daml is reached.

Set Up the Example Projects
***************************

To set up the example projects, clone the public GitHub repository at `github.com/digital-asset/ex-java-bindings <https://github.com/digital-asset/ex-java-bindings>`__ and follow the setup instruction in the `README file <https://github.com/digital-asset/ex-java-bindings/blob/master/README.rst#setting-up-the-example-projects>`__.

This project contains two examples of the PingPong application, built directly with gRPC and using the RxJava2-based Java bindings.

Example Project
***************

PingPongMain.java
=================

The entry point for the Java code is the main class ``src/main/java/examples/pingpong/grpc/PingPongMain.java``. Look at this class to see:

- how to connect to and interact with a Daml Ledger via the Java bindings
- how to use the Reactive layer to build an automation for both parties.

At high level, the code does the following steps:

- creates an instance of ``DamlLedgerClient`` connecting to an existing Ledger
- connect this instance to the Ledger with ``DamlLedgerClient.connect()``
- create two instances of ``PingPongProcessor``, which contain the logic of the automation

  (This is where the application reacts to the new ``Ping`` or ``Pong`` contracts.)
- run the ``PingPongProcessor`` forever by connecting them to the incoming transactions
- inject some contracts for each party of both templates
- wait until the application is done

PingPongProcessor.runIndefinitely()
===================================

The core of the application is the ``PingPongProcessor.runIndefinitely()``.

The ``PingPongProcessor`` queries the transactions first via the ``TransactionsClient`` of the ``DamlLedgerClient``. Then, for each transaction, it produces ``Commands`` that will be sent to the Ledger via the ``CommandSubmissionClient`` of the ``DamlLedgerClient``.

Output
======

The application prints statements similar to these:

.. code-block:: text

    Bob is exercising RespondPong on #1:0 in workflow Ping-Alice-1 at count 0
    Alice is exercising RespondPing on #344:1 in workflow Ping-Alice-7 at count 9

The first line shows that:

- ``Bob`` is exercising the ``RespondPong`` choice on the contract with ID ``#1:0`` for the workflow ``Ping-Alice-1``.
- Count ``0`` means that this is the first choice after the initial ``Ping`` contract.
- The workflow ID  ``Ping-Alice-1`` conveys that this is the workflow triggered by the second initial ``Ping`` contract that was created by ``Alice``.

The second line is analogous to the first one.

