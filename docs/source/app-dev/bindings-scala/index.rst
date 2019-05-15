.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Scala bindings
##############

This page provides a basic Scala programmer's introduction to working with Digital Asset distributed ledger,
using the Scala programming language and the **Ledger API**.

Introduction
============

The Scala bindings is a client implementation of the **Ledger API**. The Scala bindings library lets you write applications that connect to the Digital Asset distributed ledger using the Scala programming language.

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

Getting started
===============

If this is your first experience with the Scala bindings library, we recommend that you start by looking at the
`quickstart-scala example <https://github.com/digital-asset/daml/tree/master/language-support/scala/examples/quickstart-scala>`_.

To use the Scala bindings, set up the following dependencies in your project:

.. literalinclude:: ./code-snippets/quickstart-scala/build.sbt
   :start-after: // <doc-ref:dependencies>
   :end-before: // </doc-ref:dependencies>

We recommend separating generated code and application code into different modules. There are two modules in the example below:

- ``scala-codegen``
    This modules contains all generated Scala classes.
- ``application``
    This is the application code that makes use of the generated Scala classes.

.. literalinclude:: ./code-snippets/quickstart-scala/build.sbt
   :start-after: // <doc-ref:modules>
   :end-before: // </doc-ref:modules>

``scala-codegen`` module uses the following function to generate Scala classes from a DAR file.

.. literalinclude:: ./code-snippets/quickstart-scala/build.sbt
   :start-after: // <doc-ref:generate-scala>
   :end-before: // </doc-ref:generate-scala>

You can get the entire `build.sbt file <https://github.com/digital-asset/daml/blob/master/language-support/scala/examples/quickstart-scala/build.sbt>`__ from the ``daml`` repository on GitHub.

Generating Scala code from the command line
===========================================

The above example demonstrates how to use Scala codegen from **sbt**. You can also call Scala codegen directly
from a command line.


1) Install :doc:`the latest version of the DAML SDK </getting-started/installation>`.

2) Download `the latest version of the Scala codegen command line interface <https://bintray.com/api/v1/content/digitalassetsdk/DigitalAssetSDK/com/daml/codegen-main/$latest/codegen-main-$latest.jar?bt_package=sdk-components>`_.

3) Build a **DAR** file from a **DAML** model. Refer to :ref:`assistant-manual-building-dars` for more instructions.

4) Run Scala codegen::

    $ java -jar <parth-to-codegen-main-jar> scala <path-to-DAR-file>=<package-name> \
        --output-directory=<path-to-output-directory> --verbosity=<0|1|2|3|4>

Here is an example, assuming SDK Version: **0.12.17**, DAR file: **./quickstart-scala.dar**,
package name: **com.digitalasset.quickstart.iou.model**, codegen output directory: **./codegen-out** and
verbosity level: **2** (INFO)::

    $ java -jar codegen-main-100.12.17.jar scala ./quickstart-scala.dar=com.digitalasset.quickstart.iou.model \
        --output-directory=./codegen-out --verbosity=2
    ...
    [INFO ] Scala Codegen result:
    Number of generated templates: 3
    Number of not generated templates: 0
    Details:

The output above tells that codegen produced Scala classes for 3 templates without errors (empty ``Details:`` line).

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
