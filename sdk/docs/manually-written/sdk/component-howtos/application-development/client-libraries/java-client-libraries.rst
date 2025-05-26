.. _component-howtos-application-development-java-client-libraries:

Java client libraries
=====================

The Java client libraries offer a client implementation of the Ledger API
based on `RxJava <https://github.com/ReactiveX/RxJava>`_, a library for composing asynchronous and event-based programs
using observable sequences for the Java VM. They also provide an idiomatic way to write Canton Network applications in
Java that simplifies interacting with the Ledger API.

The two Java client libraries work on two different layers:

.. _component-howtos-application-development-java-client-libraries-bindings-java:

- The data layer (``bindings-java``):
    A Java-idiomatic layer based on the Ledger API generated classes. This layer simplifies the code required to work
    with the Ledger API. It contains the toProto and fromProto methods to interact with the generated classes for the
    gRPC Ledger API.
    Additionally, it provides a set of classes that represent the basic Daml data types in Java
    and are utilized by the :ref:`Daml Codegen for Java <component-howtos-application-development-daml-codegen-java>`
    to generate user-defined Daml code equivalent in Java.

    This layer is implemented in the Java package ``com.daml.ledger.javaapi.data``.

.. _component-howtos-application-development-java-client-libraries-bindings-rxjava:

- The reactive layer (``bindings-rxjava``):
    A thin layer built on top of the Ledger API services generated classes.

    For each Ledger API service, there is a reactive counterpart with a matching name. For instance, the reactive
    counterpart of ``StateServiceGrpc`` is ``StateClient``.

    The reactive layer also exposes the main interface representing a client connecting via the gRPC Ledger API. This
    interface is called ``LedgerClient`` and the main implementation working against a Canton Ledger is the
    ``DamlLedgerClient``.

    This layer is implemented in the Java package ``com.daml.ledger.rxjava``.

Look at :ref:`How to work with contracts and transactions in Java <howto-applications-work-with-contracts-java>` for
details on how to use both libraries to write Canton Network applications in Java.

References
-------------

See the :brokenref:`JavaDoc reference documentation <javadocs/index.html>`.
