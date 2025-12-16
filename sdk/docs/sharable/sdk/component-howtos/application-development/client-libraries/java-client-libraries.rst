.. _component-howtos-application-development-java-client-libraries:

Java client libraries
=====================

The Java client libraries simplify using the :externalref:`gRPC Ledger API <ledger-api-services>`.
Read :ref:`How to work with contracts and transactions in Java <howto-applications-work-with-contracts-java>` for
guidance on how to use them. Read below to learn about their code architecture, installation, and reference materials.

Overview
--------

.. _component-howtos-application-development-java-client-libraries-bindings-java:

The Java client library (``bindings-java``) is a Java-idiomatic layer based on the Ledger API generated classes.
This layer simplifies the code required to work with the Ledger API. It contains the ``toProto`` and ``fromProto``
methods to interact with the generated classes for the gRPC Ledger API.

Additionally, it provides a set of classes that represent the basic Daml data types in Java and are utilized by the
:ref:`Daml Codegen for Java <component-howtos-application-development-daml-codegen-java>` to generate user-defined Daml
code equivalent in Java.

This library is implemented in the Java package ``com.daml.ledger.javaapi.data``.

Install
-------

Find the available versions of the Java client libraries in the Maven Central Repository:

`bindings-java <https://search.maven.org/artifact/com.daml/bindings-java>`_


References
-------------

See the `JavaDoc reference documentation </javadocs/3.4/index.html>`_.
