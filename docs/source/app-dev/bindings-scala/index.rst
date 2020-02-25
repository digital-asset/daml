.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Scala bindings
##############

This page provides a basic Scala programmer's introduction to working with DAML Ledgers,
using the Scala programming language and the **Ledger API**.

Introduction
============

The Scala bindings is a client implementation of the **Ledger API**. The Scala bindings library lets you write applications that connect to a DAML Ledger using the Scala programming language.

There are two main components:

- Scala codegen
    DAML to Scala code generator. Use this to generate Scala classes from DAML models. The generated Scala code provides a type safe way of creating contracts (:ref:`com.digitalasset.ledger.api.v1.CreateCommand`) and exercising contract choices (:ref:`com.digitalasset.ledger.api.v1.ExerciseCommand`).

- Akka Streams-based API
    The API that you use to send commands to the ledger and receive transactions back.

In order to use the Scala bindings, you should be familiar with:

- :doc:`DAML language </daml/reference/index>`
- :doc:`Ledger API </app-dev/grpc/index>`
- `Akka Streams API <https://doc.akka.io/docs/akka/current/stream/index.html>`_
- `Scala programming language <https://www.scala-lang.org>`_
- :ref:`assistant-manual-building-dars`
- :doc:`DAML codegen </tools/codegen>`

Getting started
===============

If this is your first experience with the Scala bindings library, we recommend that you start by looking at the
`quickstart-scala example <https://github.com/digital-asset/daml/tree/master/language-support/scala/examples/quickstart-scala>`_.

To use the Scala bindings, set up the following dependencies in your project:

.. literalinclude:: ./code-snippets/quickstart-scala/build.sbt
   :start-after: // <doc-ref:dependencies>
   :end-before: // </doc-ref:dependencies>

We recommend separating generated code and application code into different modules. There are two modules in the ``quickstart-scala`` example:

- ``scala-codegen``
    This module will contain only generated Scala classes.
- ``application``
    This is the application code that makes use of the generated Scala classes.

.. literalinclude:: ./code-snippets/quickstart-scala/build.sbt
   :start-after: // <doc-ref:modules>
   :end-before: // </doc-ref:modules>

Generating Scala code
=====================

1) Install :doc:`the latest version of the DAML SDK </getting-started/installation>`.

2) Build a **DAR** file from a **DAML** model. Refer to :ref:`assistant-manual-building-dars` for more instructions.

3) Configure ``codegen`` in the ``daml.yaml`` (for more details see :doc:`DAML codegen </tools/codegen>` documentation).

.. literalinclude:: ./code-snippets/quickstart-scala/daml.yaml
   :start-after: # <doc-ref:codegen-scala>
   :end-before: # </doc-ref:codegen-scala>

4) Run Scala codegen::

    $ daml codegen scala

If the command is successful, it should print::

    Scala codegen
    Reading configuration from project configuration file
    [INFO ] Scala Codegen verbosity: INFO
    [INFO ] decoding archive with Package ID: 5c96aa21d5f38386833ff47fe1a7562afb5b3fe5be520f289c42892dfb0ef42b
    [INFO ] decoding archive with Package ID: 748d55be531976e941076a44fe8c06ad4a7bdb36160711dd0204b5ab8dc77e44
    [INFO ] decoding archive with Package ID: d841a5e45897aea965ab7699f3e51613c9d00b9fbd1bb09658d7fb00486f5b57
    [INFO ] Scala Codegen result:
    Number of generated templates: 3
    Number of not generated templates: 0
    Details:

The output above tells that Scala codegen read configuration from ``daml.yaml`` and produced Scala classes for 3 templates without errors (empty ``Details:`` line).

Example code
============

In this section we will demonstrate how to use the Scala bindings library.

This section refers to the IOU DAML example from the :doc:`Quickstart guide </getting-started/quickstart>` and
`quickstart-scala example <https://github.com/digital-asset/daml/tree/master/language-support/scala/examples/quickstart-scala>`_ that we already mentioned above.

Please keep in mind that **quickstart-scala example** compiles with ``-Xsource:2.13`` **scalac** option, this is to activate the fix for a Scala bug that forced users to add extra imports for implicits that should not be needed.

Create a contract and send a CreateCommand
------------------------------------------

To create a Scala class representing an **IOU** contract, you need the following **imports**:

.. literalinclude:: ./code-snippets/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/IouMain.scala
   :start-after: // <doc-ref:imports>
   :end-before: // </doc-ref:imports>

the definition of the **issuer** ``Party``:

.. literalinclude:: ./code-snippets/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/IouMain.scala
   :start-after: // <doc-ref:issuer-definition>
   :end-before: // </doc-ref:issuer-definition>

and the following code to create an instance of the ``M.Iou`` class:

.. literalinclude:: ./code-snippets/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/IouMain.scala
   :start-after: // <doc-ref:iou-contract-instance>
   :end-before: // </doc-ref:iou-contract-instance>

To send a :ref:`com.digitalasset.ledger.api.v1.CreateCommand` (keep in mind the following code snippet is part of the Scala *for comprehension expression*):

.. literalinclude:: ./code-snippets/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/IouMain.scala
   :start-after: // <doc-ref:submit-iou-create-command>
   :end-before: // </doc-ref:submit-iou-create-command>

For more details on how to submit a command, please refer to the implementation of `com.digitalasset.quickstart.iou.ClientUtil#submitCommand <https://github.com/digital-asset/daml/blob/master/language-support/scala/examples/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/ClientUtil.scala>`_.

Receive a transaction, exercise a choice and send an ExerciseCommand
---------------------------------------------------------------------

To receive a transaction as a **newOwner** and decode a :ref:`com.digitalasset.ledger.api.v1.CreatedEvent` for ``IouTransfer`` contract, you need the definition of the **newOwner** ``Party``:

.. literalinclude:: ./code-snippets/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/IouMain.scala
   :start-after: // <doc-ref:new-owner-definition>
   :end-before: // </doc-ref:new-owner-definition>

and the following code that handles subscription and decoding:

.. literalinclude:: ./code-snippets/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/IouMain.scala
   :start-after: // <doc-ref:subscribe-and-decode-iou-transfer>
   :end-before: // </doc-ref:subscribe-and-decode-iou-transfer>

To exercise ``IouTransfer_Accept`` choice on the ``IouTransfer`` contract that you received and send a corresponding :ref:`com.digitalasset.ledger.api.v1.ExerciseCommand`:

.. literalinclude:: ./code-snippets/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/IouMain.scala
   :start-after: // <doc-ref:submit-iou-transfer-accept-exercise-command>
   :end-before: // </doc-ref:submit-iou-transfer-accept-exercise-command>

Fore more details on how to subscribe to receive events for a particular party, please refer to the implementation of `com.digitalasset.quickstart.iou.IouMain#newOwnerAcceptsAllTransfers <https://github.com/digital-asset/daml/blob/master/language-support/scala/examples/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/IouMain.scala>`_.


.. _scala-bindings-authentication:

Authentication
==============

Some ledgers will require you to send an access token along with each request.
To learn more about authentication, read the :doc:`Authentication </app-dev/authentication>` overview.

To use the same token for all ledger API requests,
use the ``token`` field of ``LedgerClientConfiguration``:

.. literalinclude:: ./code-snippets/quickstart-scala/application/src/main/scala/com/digitalasset/quickstart/iou/IouMain.scala
   :start-after: // <doc-ref:ledger-client-configuration>
   :end-before: // </doc-ref:ledger-client-configuration>

To specify the token for an individual call,
use the ``token`` parameter:

.. code-block:: scala

   transactionClient.getLedgerEnd() // Uses the token specified in LedgerClientConfiguration
   transactionClient.getLedgerEnd(token = acessToken) // Uses the given token

Note that if your tokens can change at run time (e.g., because they expire or because you switch users),
you will need to specify them on a per-call basis as shown above.
