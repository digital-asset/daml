.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _bindings-java-tutorial:

Java Binding
############

.. toctree::
  :hidden:

  codegen

The Java Binding is a client implementation of the *Ledger API*
:ref:`based on RxJava <ledger-api-java-binding-underlying-library>`. It provides an
idiomatic way to write DAML Ledger applications.

.. seealso::
   This documentation for the Java Binding API includes the `JavaDoc reference documentation <javadocs/index.html>`_.

Overview
********

The Java Binding library is composed of:

- The Data Layer
    A Java-idiomatic layer based on the Ledger API generated classes.
    This layer simplifies the code required to work with the Ledger API.

    Can be found in the java package ``com.daml.ledger.javaapi.data``.
- The Reactive Layer
    A thin layer built on top of the Ledger API services generated classes.

    For each Ledger API service, there is a reactive counterpart with a 
    matching name. For instance, the reactive counterpart of ``ActiveContractsServiceGrpc``
    is ``ActiveContractsClient``.

    The Reactive Layer also exposes the main interface representing a client
    connecting via the Ledger API. This interface is called ``LedgerClient`` and the
    main implementation working against the DAML Ledger is the ``DamlLedgerClient``.

    Can be found in the java package ``com.daml.ledger.rxjava``.
- The Reactive Components
    A set of optional components you can use to assemble DAML Ledger applications. 

    The most important components are: 

    - the ``LedgerView``, which provides a local view of the Ledger
    - the ``Bot``, which provides utility methods to assemble automation logic for the Ledger

    Can be found in the java package ``com.daml.ledger.rxjava.components``.

LedgerClient
============

Connections to the ledger are made by creating instance of classes that implement the interface ``LedgerClient``. The class ``DamlLedgerClient`` implements this interface, and is used to connect to a DA ledger.

This class provides access to the ledgerId, and all clients that give access to the various ledger services, such as the active contract set, the transaction service, the time service, etc. This is described :ref:`below <ledger-api-java-binding-connecting>`. Consult the `JavaDoc for DamlLedgerClient <javadocs/com/daml/ledger/rxjava/DamlLedgerClient.html>`_ for full details.

LedgerView
==========

The ``LedgerView`` of an application is the "copy" of the ledger that the application has locally. You
can query it to obtain the contracts that are active on the Ledger and not pending. 

.. note::

  - A contract is *active* if it exists in the Ledger and has not yet been archived.
  - A contract is *pending* if the application has sent a consuming command to the Ledger and has yet
    to receive an completion for the command (that is, if the command has succeeded or not).

The ``LedgerView`` is updated every time: 

- a new event is received from the Ledger
- new commands are sent to the Ledger
- a command has failed to be processed

For instance, if an incoming transaction is received with a create event for a contract that is relevant
for the application, the application ``LedgerView`` is updated to contain that contract too.

Bot
===

The ``Bot`` is an abstraction used to write automation for the DAML Ledger. It is conceptually
defined by two aspects: 

- the ``LedgerView``
- the logic that produces commands, given a ``LedgerView`` 

When the ``LedgerView`` is updated, to see if the bot has new commands to submit based on the
updated view, the logic of the bot is run.

The logic of the bot is a Java function from the bot's ``LedgerView`` to a ``Flowable<CommandsAndPendingSet>``.
Each ``CommandsAndPendingSet`` contains:

- the commands to send to the Ledger
- the set of contractIds that should be considered pending while the command is in-flight
  (that is, sent by the client but not yet processed by the Ledger)

You can wire a ``Bot`` to a ``LedgerClient`` implementation using ``Bot.wire``:

.. code-block:: java

    Bot.wire(String applicationId,
             LedgerClient ledgerClient,
             TransactionFilter transactionFilter,
             Function<LedgerViewFlowable.LedgerView<R>, Flowable<CommandsAndPendingSet>> bot,
             Function<CreatedContract, R> transform)

In the above:

- ``applicationId``
    The id used by the Ledger to identify all the queries from the same application.
- ``ledgerClient`` 
    The connection to the Ledger.
- ``transactionFilter``
    The server-side filter to the incoming transactions. Used to reduce the traffic between
    Ledger and application and make an application more efficient.
- ``bot``
    The logic of the application,
- ``transform``
    The function that, given a new contract, returns which information for 
    that contracts are useful for the application. Can be used to reduce space used
    by discarding all the info not required by the application. The input to the function
    contains the ``templateId``, the arguments of the contract created and the context of
    the created contract. The context contains the ``workflowId``.


Getting started
***************

The Java Binding library can be added to a `Maven <https://maven.apache.org/>`_ project.
Read :ref:`setup-maven-project` to configure your machine.

.. _bindings-java-setup-maven:

Setup a Maven project
=====================

To use the Java Binding library, add the following dependencies to your project's ``pom.xml``:

.. code-block:: xml

    <dependency>
        <groupId>com.daml.ledger</groupId>
        <artifactId>bindings-java</artifactId>
        <version>x.y.z</version>
    </dependency>

    <dependency>
        <groupId>com.daml.ledger</groupId>
        <artifactId>bindings-rxjava</artifactId>
        <version>x.y.z</version>
    </dependency>

Replace ``x.y.z`` for both dependencies with the version that you want to use. You can find the available versions at
`https://digitalassetsdk.bintray.com/DigitalAssetSDK/com/daml/ledger/`.

.. _ledger-api-java-binding-connecting:

Connecting to the ledger
========================

Before any ledger services can be accessed, a connection to the ledger must be established. This is done by creating a instance of a ``DamlLedgerClient`` using one of the factory methods ``DamlLedgerClient.forLedgerIdAndHost`` and ``DamlLedgerClient.forHostWithLedgerIdDiscovery``. This instance can then be used to access service clients directly, or passed to a call to ``Bot.wire`` to connect a ``Bot`` instance to the ledger.

.. _ledger-api-java-binding-connecting-securely:

Connecting securely
===================

The Java Binding library lets you connect to a DAML Ledger via a secure connection. The factory methods
``DamlLedgerClient.forLedgerIdAndHost`` and ``DamlLedgerClient.forHostWithLedgerIdDiscovery`` accept a parameter of type ``Optional<SslContext>``.
If the value of that optional parameter is not present (i.e. by passing ``Optional.empty()``), a plain text / insecure connection will be established.
This is useful when connecting to a locally running Sandbox.

Secure connections to a DAML Ledger must be configured to use client authentication certificates, which can be provided by a Ledger Operator.

For information on how to set up an ``SslContext`` with the provided certificates for client authentication, please consult the gRPC documentation on
`TLS with OpenSSL <https://github.com/grpc/grpc-java/blob/master/SECURITY.md#tls-with-openssl>`_ as well as the
`HelloWorldClientTls <https://github.com/grpc/grpc-java/blob/70b1b1696a258ffe042c7124217e3a7894821444/examples/src/main/java/io/grpc/examples/helloworldtls/HelloWorldClientTls.java#L46-L57>`_ example of the ``grpc-java`` project.

Advanced connection settings
============================
Sometimes the default settings for gRPC connections/channels are not suitable for a given situation. These usecases are supported by creating a a custom `ManagedChannel <https://grpc.io/grpc-java/javadoc/io/grpc/ManagedChannel.html>`_ object via `ManagedChannelBuilder <https://grpc.io/grpc-java/javadoc/io/grpc/ManagedChannelBuilder.html>`_  or `NettyChannelBuilder <https://grpc.io/grpc-java/javadoc/io/grpc/netty/NettyChannelBuilder.html>`_ and passing the channel instance to the constructor of `DamlLedgerClient <javadocs/com/daml/ledger/rxjava/DamlLedgerClient.html>`_.

Example
=======

To try out the Java Binding library, use the `examples on GitHub <https://github.com/digital-asset/ex-java-bindings>`__: ``PingPongReactive`` or ``PingPongComponents``.

The former example does not use the Reactive Components, and the latter example does.
Both examples implement the ``PingPong`` application, which consists of:

- a DAML model with two contract templates, ``Ping`` and ``Pong``
- two parties, ``Alice`` and ``Bob``

The logic of the application is the following:

#. The application injects a contract of type ``Ping`` for ``Alice``.
#. ``Alice`` sees this contract and exercises the consuming choice ``RespondPong`` to create a contract
   of type ``Pong`` for ``Bob``.
#. ``Bob`` sees this contract and exercises the consuming choice ``RespondPing``  to create a contract
   of type ``Ping`` for ``Alice``.
#. Points 1 and 2 are repeated until the maximum number of contracts defined in the DAML is
   reached.

Setting up the example projects
===============================

To set up the example projects, clone the public GitHub repository at `github.com/digital-asset/ex-java-bindings <https://github.com/digital-asset/ex-java-bindings>`__
and follow the setup instruction in the `README file <https://github.com/digital-asset/ex-java-bindings/blob/master/README.rst#setting-up-the-example-projects>`__. This project contains three examples of the PingPong application, built
with gRPC (non-Reactive), Reactive and Reactive Component bindings respectively.

Example project -- Ping Pong without reactive components
========================================================

PingPongMain.java
-----------------

The entry point for the Java code is the main class ``src/main/java/examples/pingpong/PingPongMain.java``.
Look at this class to see:

- how to connect to and interact with the DML Ledger via the Java Binding library
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
-----------------------------------

The core of the application is the ``PingPongProcessor.runIndefinitely()``.

The ``PingPongProcessor`` queries the transactions first via the ``TransactionsClient``
of the ``DamlLedgerClient``. Then, for each
transaction, it produces ``Commands`` that will be sent to the Ledger via the ``CommandSubmissionClient``
of the ``DamlLedgerClient``.

Output
------

The application prints statements similar to these:

.. code-block:: text

    Bob is exercising RespondPong on #1:0 in workflow Ping-Alice-1 at count 0
    Alice is exercising RespondPing on #344:1 in workflow Ping-Alice-7 at count 9

The first line shows that:

- ``Bob`` is exercising the ``RespondPong`` choice on the contract with ID ``#1:0`` for the workflow ``Ping-Alice-1``.
- Count ``0`` means that this is the first choice after the initial ``Ping`` contract.
- The workflow ID  ``Ping-Alice-1`` conveys that this is the workflow triggered by the second initial ``Ping``
  contract that was created by ``Alice``.

The second line is analogous to the first one.

Example project -- Ping Pong with reactive components
=====================================================

PingPongMain.java
-----------------

The entry point for the Java code is the main class ``src/main/java/examples/pingpong/PingPongMain.java``.
Look at this class to see:

- how to connect to and interact with the DML Ledger via the Java Binding library 
- how to use the Reactive Components to build an automation for both parties

PingPongBot
-----------

At high level, this application follows the same steps as the one without Reactive Components
except for the ``PingPongProcessor``. In this application, the ``PingPongProcessor`` is replaced by
the ``PingPongBot``.

The ``PingPongBot`` has two important methods:

- ``getContractInfo(Record record, TransactionContext context)`` which is used to get the
  information useful to the application from a created contract and the context
- ``process(LedgerView<ContractInfo> ledgerView)`` which implements the logic of the application
  by converting the local view of the Ledger into a stream of ``Commands``

Output
------

The application prints statements similar to the ones seen in the section above.

.. _ledger-api-java-binding-underlying-library:

The underlying library: RxJava
******************************

The Java Binding is `RxJava <https://github.com/ReactiveX/RxJava>`_, a library for
composing asynchronous and event-based programs using observable sequences for the Java VM.
It is part of the family of libraries called `ReactiveX <https://reactivex.io/>`_.

ReactiveX was chosen as the underlying library for the Java Binding because
many services that the DAML Ledger offers are exposed as streams of events.
So an application that wants to interact with the DAML Ledger must react
to one or more DAML Ledger streams.
