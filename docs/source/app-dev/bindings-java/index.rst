.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _java-bindings:

Java Bindings
#############

.. toctree::
  :hidden:

  codegen
  Ping Pong Example <example>
  Iou Quickstart Tutorial <quickstart>

The Java bindings is a client implementation of the *Ledger API*
based on `RxJava <https://github.com/ReactiveX/RxJava>`_, a library for composing asynchronous and event-based programs using observable sequences for the Java VM. It provides an idiomatic way to write Daml Ledger applications.

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
    main implementation working against a Daml Ledger is the ``DamlLedgerClient``.

    Can be found in the java package ``com.daml.ledger.rxjava``.

Generate Code
=============

When writing applications for the ledger in Java, you want to work with a representation of Daml templates and data types in Java that closely resemble the original Daml code while still being as true to the native types in Java as possible.

To achieve this, you can use Daml to Java code generator ("Java codegen") to generate Java types based on a Daml model. You can then use these types in your Java code when reading information from and sending data to the ledger.

For more information on Java code generation, see :doc:`/app-dev/bindings-java/codegen`.

Connect to the Ledger: ``LedgerClient``
=======================================

Connections to the ledger are made by creating instance of classes that implement the interface ``LedgerClient``. The class ``DamlLedgerClient`` implements this interface, and is used to connect to a Daml ledger.

This class provides access to the ledgerId, and all clients that give access to the various ledger services, such as the active contract set, the transaction service, the time service, etc. This is described :ref:`below <ledger-api-java-binding-connecting>`. Consult the `JavaDoc for DamlLedgerClient <javadocs/com/daml/ledger/rxjava/DamlLedgerClient.html>`_ for full details.

Reference Documentation
***********************

`Click here for the JavaDoc reference documentation <javadocs/index.html>`_.

Get Started
***********

The Java bindings library can be added to a `Maven <https://maven.apache.org/>`_ project.

.. _bindings-java-setup-maven:

Set Up a Maven Project
======================

To use the Java bindings library, add the following dependencies to your project's ``pom.xml``:

.. literalinclude:: ./code-snippets/pom.xml
    :language: XML
    :start-after: <!-- start snippet: dependencies -->
    :end-before: <!-- end snippet: dependencies -->
    :dedent: 4

Replace ``x.y.z`` for both dependencies with the version that you want to use. You can find the available versions by checking
the `Maven Central Repository <https://search.maven.org/artifact/com.daml/bindings-java>`__.

You can also take a look at the ``pom.xml`` file from the :ref:`quickstart project <quickstart>`.

.. _ledger-api-java-binding-connecting:

Connect to the Ledger
=====================

Before any ledger services can be accessed, you must establish a connection to the ledger by creating an instance of a ``DamlLedgerClient``. To create an instance of a ledger client, use the static ``newBuilder(..)`` method to create a ``DamlLedgerClient.Builder``. Then use the builder instance to create the ``DamlLedgerClient``. Finally, call the ``connect()`` method on the client.

.. code-block:: java

    // Create a client object to access services on the ledger.
    DamlLedgerClient client = DamlLedgerClient.newBuilder(ledgerhost, ledgerport).build();

    // Connects to the ledger and runs initial validation.
    client.connect();

.. _ledger-api-java-bindings-authorization:

Perform Authorization
=====================

Some ledgers will require you to send an access token along with each request.

To learn more about authorization, read the :doc:`Authorization </app-dev/authorization>` overview.

To use the same token for all Ledger API requests, the ``DamlLedgerClient`` builders expose a ``withAccessToken`` method. This will allow you to not pass a token explicitly for every call.

If your application is long-lived and your tokens are bound to expire, you can reload the necessary token when needed and pass it explicitly for every call. Every client method has an overload that allows a token to be passed, as in the following example:

.. code-block:: java

   transactionClient.getLedgerEnd(); // Uses the token specified when constructing the client
   transactionClient.getLedgerEnd(accessToken); // Override the token for this call exclusively

If you're communicating with a ledger that verifies authorization it's very important to secure the communication channel to prevent your tokens to be exposed to man-in-the-middle attacks. The next chapter describes how to enable TLS.

.. _ledger-api-java-binding-connecting-securely:

Connect Securely
================

The Java bindings library lets you connect to a Daml Ledger via a secure connection. The builders created by
``DamlLedgerClient.newBuilder`` default to a plaintext connection, but you can invoke ``withSslContext`` to pass an ``SslContext``.
Using the default plaintext connection is useful only when connecting to a locally running Sandbox for development purposes.

Secure connections to a Daml Ledger must be configured to use client authentication certificates, which can be provided by a Ledger Operator.

For information on how to set up an ``SslContext`` with the provided certificates for client authentication, please consult the gRPC documentation on
`TLS with OpenSSL <https://github.com/grpc/grpc-java/blob/master/SECURITY.md#tls-with-openssl>`_ as well as the
`HelloWorldClientTls <https://github.com/grpc/grpc-java/blob/70b1b1696a258ffe042c7124217e3a7894821444/examples/src/main/java/io/grpc/examples/helloworldtls/HelloWorldClientTls.java#L46-L57>`_ example of the ``grpc-java`` project.

Advanced Connection Settings
============================

Sometimes the default settings for gRPC connections/channels are not suitable for a given situation. These use cases are supported by creating a custom `NettyChannelBuilder <https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyChannelBuilder.html>`_ object and passing the it to the ``newBuilder`` static method defined over `DamlLedgerClient <javadocs/com/daml/ledger/rxjava/DamlLedgerClient.html>`_.

Example Projects
****************

Example projects using the Java bindings are available on `GitHub <https://github.com/digital-asset/ex-java-bindings>`__. :doc:`Read more about them here </app-dev/bindings-java/example>`.

