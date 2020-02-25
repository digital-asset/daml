.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Java bindings
#############

.. toctree::
  :hidden:

  codegen
  example

The Java bindings is a client implementation of the *Ledger API*
based on `RxJava <https://github.com/ReactiveX/RxJava>`_, a library for composing asynchronous and event-based programs using observable sequences for the Java VM. It provides an idiomatic way to write DAML Ledger applications.

.. seealso::
   This documentation for the Java bindings API includes the `JavaDoc reference documentation <javadocs/index.html>`_.

Overview
********

The Java bindings library is composed of:

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
    main implementation working against a DAML Ledger is the ``DamlLedgerClient``.

    Can be found in the java package ``com.daml.ledger.rxjava``.
- The Reactive Components
    A set of optional components you can use to assemble DAML Ledger applications. 

    The most important components are: 

    - the ``LedgerView``, which provides a local view of the Ledger
    - the ``Bot``, which provides utility methods to assemble automation logic for the Ledger

    Can be found in the java package ``com.daml.ledger.rxjava.components``.

Code generation
===============

When writing applications for the ledger in Java, you want to work with a representation of DAML templates and data types in Java that closely resemble the original DAML code while still being as true to the native types in Java as possible.

To achieve this, you can use DAML to Java code generator ("Java codegen") to generate Java types based on a DAML model. You can then use these types in your Java code when reading information from and sending data to the ledger.

For more information on Java code generation, see :doc:`/app-dev/bindings-java/codegen`.

Connecting to the ledger: LedgerClient
======================================

Connections to the ledger are made by creating instance of classes that implement the interface ``LedgerClient``. The class ``DamlLedgerClient`` implements this interface, and is used to connect to a DAML ledger.

This class provides access to the ledgerId, and all clients that give access to the various ledger services, such as the active contract set, the transaction service, the time service, etc. This is described :ref:`below <ledger-api-java-binding-connecting>`. Consult the `JavaDoc for DamlLedgerClient <javadocs/com/daml/ledger/rxjava/DamlLedgerClient.html>`_ for full details.

Accessing data on the ledger: LedgerView
========================================

The ``LedgerView`` of an application is the "copy" of the ledger that the application has locally. You can query it to obtain the contracts that are active on the Ledger and not pending. 

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

Writing automations: Bot
========================

The ``Bot`` is an abstraction used to write automation for a DAML Ledger. It is conceptually
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

Reference documentation
***********************

`Click here for the JavaDoc reference documentation <javadocs/index.html>`_.

Getting started
***************

The Java bindings library can be added to a `Maven <https://maven.apache.org/>`_ project.

.. _bindings-java-setup-maven:

Set up a Maven project
======================

To use the Java bindings library, add the following dependencies to your project's ``pom.xml``:

.. literalinclude:: ./code-snippets/pom.xml
    :language: XML
    :start-after: <!-- start snippet: dependencies -->
    :end-before: <!-- end snippet: dependencies -->
    :dedent: 4

Replace ``x.y.z`` for both dependencies with the version that you want to use. You can find the available versions by checking
the `Maven Central Repository <https://search.maven.org/search?q=g:com.daml.ledger>`__.

.. note::
   As of DAML SDK release 0.13.3, the Java Bindings libraries are available via the public Maven Central repository. Earlier releases are available from the `DAML Bintray repository <https://digitalassetsdk.bintray.com>`__.

You can also take a look at the ``pom.xml`` file from the :ref:`quickstart project <quickstart>`.

.. _ledger-api-java-binding-connecting:

Connecting to the ledger
========================

Before any ledger services can be accessed, a connection to the ledger must be established. This is done by creating a instance of a ``DamlLedgerClient`` using one of the factory methods ``DamlLedgerClient.forLedgerIdAndHost`` and ``DamlLedgerClient.forHostWithLedgerIdDiscovery``. This instance can then be used to access service clients directly, or passed to a call to ``Bot.wire`` to connect a ``Bot`` instance to the ledger.

.. _ledger-api-java-bindings-authentication:

Authenticating
==============

Some ledgers will require you to send an access token along with each request.

To learn more about authentication, read the :doc:`Authentication </app-dev/authentication>` overview.

To use the same token for all Ledger API requests, the ``DamlLedgerClient`` builders expose a ``withAccessToken`` method. This will allow you to not pass a token explicitly for every call.

If your application is long-lived and your tokens are bound to expire, you can reload the necessary token when needed and pass it explicitly for every call. Every client method has an overload that allows a token to be passed, as in the following example:

.. code-block:: java

   transactionClient.getLedgerEnd(); // Uses the token specified when constructing the client
   transactionClient.getLedgerEnd(accessToken); // Override the token for this call exclusively

If you're communicating with a ledger protected by authentication it's very important to secure the communication channel to prevent your tokens to be exposed to man-in-the-middle attacks. The next chapter describes how to enable TLS.

.. _ledger-api-java-binding-connecting-securely:

Connecting securely
===================

The Java bindings library lets you connect to a DAML Ledger via a secure connection. The builders created by
``DamlLedgerClient.newBuilder`` default to a plaintext connection, but you can invoke ``withSslContext` to pass an ``SslContext``.
Using the default plaintext connection is useful only when connecting to a locally running Sandbox for development purposes.

Secure connections to a DAML Ledger must be configured to use client authentication certificates, which can be provided by a Ledger Operator.

For information on how to set up an ``SslContext`` with the provided certificates for client authentication, please consult the gRPC documentation on
`TLS with OpenSSL <https://github.com/grpc/grpc-java/blob/master/SECURITY.md#tls-with-openssl>`_ as well as the
`HelloWorldClientTls <https://github.com/grpc/grpc-java/blob/70b1b1696a258ffe042c7124217e3a7894821444/examples/src/main/java/io/grpc/examples/helloworldtls/HelloWorldClientTls.java#L46-L57>`_ example of the ``grpc-java`` project.

Advanced connection settings
============================

Sometimes the default settings for gRPC connections/channels are not suitable for a given situation. These use cases are supported by creating a a custom `NettyChannelBuilder <https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyChannelBuilder.html>`_ object and passing the it to the ``newBuilder`` static method defined over `DamlLedgerClient <javadocs/com/daml/ledger/rxjava/DamlLedgerClient.html>`_.

Example project
***************

Example projects using the Java bindings are available on `GitHub <https://github.com/digital-asset/ex-java-bindings>`__. :doc:`Read more about them here </app-dev/bindings-java/example>`.
