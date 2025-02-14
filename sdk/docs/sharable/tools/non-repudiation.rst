.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Non-repudiation
###############

The non-repudiation middleware, API and client library are only available in
`Daml Enterprise <https://www.digitalasset.com/products/daml-connect>`_ and are currently an
:doc:`Early Access Feature in Alpha status </support/status-definitions>`.

When you are issuing a command over the Ledger API, there is an implicit trust assumption between the issuer of the command and the operator of the participant
that the latter will not issue commands on behalf of the former.

The non-repudiation middleware and its client library are a Daml Enterprise exclusive feature that allows ledger operators to run
participant nodes that will require each command to come with a verifiable cryptographic signature, which will persisted by the operator. As the
sole owner of the private key used to sign the command, the authenticity of the command is thus verified and preserved, ensuring that an operator
cannot issue a command on behalf of the user and that the user cannot repudiate the command.

Note that this is an early access feature: its status is currently under development and further feedback can change how certain details might work
once the feature is declared a stable part of Daml Enterprise. If you are interested in this feature, you are welcome to use it and
give us feedback that will shape how this feature will ultimately come to be.

Architecture
~~~~~~~~~~~~

The non-repudiation system consists of three components:

- the non-repudiation middleware is a reverse proxy that sits in front of the Ledger API that verifies command signatures and forwards the signed command to the actual participant node
- the non-repudiation API is a web server used by the operator to upload new certificates and verify repudiation claims
- the non-repudiation client is a gRPC interceptor that can be used alongside any gRPC client on the JVM, including the official Java bindings, that will ensure that commands are signed with a given private key

Run the Server-side Components
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The server-side components are the middleware and the API. Both can be run as a single process by running the non-repudiation fat JAR provided as part of Daml Enterprise.

Note that at the current stage you need to also have a PostgreSQL server running where signed commands will be persisted.

The following example shows how to run the non-repudiation server components by connecting to a participant at localhost:6865 and proxying it to the 6866 port, using the given PostgreSQL instance to persist signed commands and certificates.

.. code-block:: sh

    java -jar /path/to/the/non-repudiation.jar --ledger-host localhost --ledger-port 6865 --proxy-port 6866 --jdbc url=jdbc:postgresql:nr,user=nr,password=nr

For details on how to run them, please run the fat JAR with the ``--help`` command line option.

Use the Client
~~~~~~~~~~~~~~

The client is a gRPC interceptor which is available to Daml Enterprise users (hence, it's not available on Maven Central).

The Maven coordinates for the library are `com.daml:non-repudiation-client`.

The following example shows how to use this interceptor with the official Java bindings

.. code-block:: java

    PrivateKey key = readYourPrivateKey();
    X509Certificate certificate = readYourX509Certificate();
    NettyChannelBuilder builder = NettyChannelBuilder.forAddress(hostname, port);
    builder.intercept(SigningInterceptor.signCommands(key, certificate));
    DamlLedgerClient client = DamlLedgerClient.newBuilder(builder).build();
    client.connect();

Non-repudiation Over the HTTP JSON API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The non-repudiation middleware acts *exclusively* as a reverse proxy in front of the Ledger API server: if you want to use the HTTP JSON API you will need to
run your own HTTP JSON API server and start it with a certificate that will be used to sign every command issued by the HTTP JSON API to the participant.

The HTTP JSON API bundled with Daml Enterprise has the following extra command line options that *must* be used to run an HTTP JSON API
server against the non-repudiation middleware:

- `--non-repudiation-certificate-path`: the path to the X.509 certificate containing the public counterpart to the private key that will be used to sign the commands
- `--non-repudiation-private-key-path`: the path to the file containing the private key that will be used to sign the commands
- `--non-repudiation-private-key-algorithm`: the name of the cryptographic algorithm of the private key (for a list of names supported in the OpenJDK: https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#KeyFactory)

TLS Support
~~~~~~~~~~~

At the current stage the non-repudiation feature does not support running against secure Ledger API servers. This will be added as part of stabilizing this feature.
