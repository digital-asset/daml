.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _howto-applications-work-with-contracts-java:

How to work with contracts and transactions in Java
===================================================

When writing Canton Network applications in Java, it is convenient to work with a representation of Daml templates and data
types in Java that closely resemble the original Daml code while still being as close to the native types of Java as
possible. To achieve this, use the :ref:`Daml Codegen for Java <component-howtos-application-development-daml-codegen-java>` to generate Java types based on a Daml
model. You can then use these types in your Java code when reading information from and sending data to the ledger.

Setup the codegen
-----------------

Run and configure the code generator for Java according to :ref:`Daml Codegen for Java <component-howtos-application-development-daml-codegen-java>` to use and
generate the Java classes for your project.

See also in :ref:`Generated code <component-howtos-application-development-daml-codegen-java-generated-code>` how the generated code looks for the Daml built-in
and user-defined types.

Use the generated classes in your project
-----------------------------------------

In order to compile the resulting Java classes, you need to add the :ref:`Java bindings library <component-howtos-application-development-java-client-libraries-bindings-java>`
as dependency to your build tools.

Add the following **Maven** dependency to your project:

.. literalinclude:: ./code-snippets/pom.xml
   :language: XML
   :start-after: <!-- start snippet: dependency bindings java -->
   :end-before: <!-- end snippet: dependency bindings java -->
   :dedent: 8

.. note::

  Replace ``YOUR_SDK_VERSION`` with the version of your SDK.

Find the available versions in the `Maven Central Repository <https://search.maven.org/artifact/com.daml/bindings-java>`__.

Access the gRPC Ledger API using Java bindings
----------------------------------------------

The :ref:`Java bindings reactive library <component-howtos-application-development-java-client-libraries-bindings-rxjava>` provides a set of classes that allow you to access the gRPC Ledger API.

For each Ledger API service, there is a reactive counterpart with a matching name. For instance, the reactive
counterpart of ``UpdateServiceGrpc`` is ``UpdateClient``.

The library also exposes the main interface representing a client connecting via the gRPC Ledger API. This interface is
called ``LedgerClient`` and the main implementation working against a Daml ledger is the ``DamlLedgerClient``.


Set up dependencies in your project
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To use the aforementioned classes, add the following dependencies to your project:

.. literalinclude:: ./code-snippets/pom.xml
   :language: XML
   :start-after: <!-- start snippet: dependency bindings rxjava -->
   :end-before: <!-- end snippet: dependency bindings rxjava -->
   :dedent: 8

.. note::

  Replace ``YOUR_SDK_VERSION`` with the version of your SDK.

Find the available versions in the `Maven Central Repository <https://search.maven.org/artifact/com.daml/bindings-rxjava>`__.

.. _howto-applications-work-with-contracts-java-connect:

Connect to the ledger
^^^^^^^^^^^^^^^^^^^^^

Create an instance of a ``DamlLedgerClient`` to establish a connection to the ledger and access the Ledger API services.
To create an instance of a ledger client, use the static ``newBuilder(..)`` method to create a ``DamlLedgerClient.Builder``.
Then use the builder instance to create the ``DamlLedgerClient``. Finally, call the ``connect()`` method on the client.

.. code-block:: java

    // Create a client object to access services on the ledger.
    DamlLedgerClient client = DamlLedgerClient.newBuilder(ledgerhost, ledgerport).build();

    // Connects to the ledger and runs initial validation.
    client.connect();

.. _howto-applications-work-with-contracts-java-authorization:

Perform authorization
^^^^^^^^^^^^^^^^^^^^^

Some ledgers enforce authorization and require to send an access token along with each request. For more details on
authorization, read the :ref:`Authorization <authorization>` overview.

To use the same token for all Ledger API requests, use the ``withAccessToken`` method of the ``DamlLedgerClient`` builder.
This allows you to not pass a token explicitly for every call.

If your application is long-lived and your tokens are bound to expire, reload the necessary token when needed and pass
it explicitly for every call. Every client method has an overload that allows a token to be passed, as in the following example:

.. code-block:: java

   stateClient.getLedgerEnd(); // Uses the token specified when constructing the client
   stateClient.getLedgerEnd(accessToken); // Override the token for this call exclusively

If you are communicating with a ledger that verifies authorization it is very important to secure the communication
channel to prevent your tokens to be exposed to man-in-the-middle attacks. The next chapter describes how to enable TLS.

.. _howto-applications-work-with-contracts-java-connect-securely:

Connect securely
^^^^^^^^^^^^^^^^

The builders created by ``DamlLedgerClient.newBuilder`` default to a plaintext connection.
The Java bindings library lets you connect to a ledger via a secure connection. To do so, invoke ``withSslContext`` and
pass an ``SslContext``.

.. warning::

  Use the default plaintext connection only when connecting to a locally running ledger for development purposes.

Secure connections to a ledger must be configured to use client authentication certificates, which can be provided by a ledger operator.

For information on how to set up an ``SslContext`` with the provided certificates for client authentication, please consult the gRPC documentation on
`TLS with OpenSSL <https://github.com/grpc/grpc-java/blob/master/SECURITY.md#tls-with-openssl>`_ as well as the
`HelloWorldClientTls <https://github.com/grpc/grpc-java/blob/70b1b1696a258ffe042c7124217e3a7894821444/examples/src/main/java/io/grpc/examples/helloworldtls/HelloWorldClientTls.java#L46-L57>`_ example of the ``grpc-java`` project.

Advanced connection settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sometimes the default settings for gRPC connections or channels are not suitable. For such situations, create a custom
`NettyChannelBuilder <https://grpc.github.io/grpc-java/javadoc/io/grpc/netty/NettyChannelBuilder.html>`_ object and
pass the it to the ``newBuilder`` static method defined over `DamlLedgerClient </javadocs/3.3/com/daml/ledger/rxjava/DamlLedgerClient.html>`_.

Alternative ways to access the Ledger API
-----------------------------------------

The Java bindings client library is not the only way to access the Ledger API. You can also use the gRPC bindings or OpenAPI
bindings directly.

Use gRPC bindings
^^^^^^^^^^^^^^^^^

For each gRPC endpoint that you want to call from your Java application, create a gRPC `StreamObserver <https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/StreamObserver.html>`_ providing
implementations of the onNext, onError and onComplete observer methods.
To decode and encode the gRPC messages, use the fromProto and toProto methods of the generated classes of the :ref:`Java
bindings library <component-howtos-application-development-java-client-libraries-bindings-java>`.


Use OpenAPI definitions
^^^^^^^^^^^^^^^^^^^^^^^

The OpenAPI definitions provide a definition for each Ledger API service and allows you to access it via the JSON
Ledger API. Use those definitions to encode/decode the gRPC messages as/from JSON payloads required by the JSON Ledger
API. For more details see :brokenref:`Get started with Canton and the JSON Ledger API < TODO(#369): add link to it>`.
