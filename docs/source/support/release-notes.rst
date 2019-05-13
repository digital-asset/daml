.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

DAML
~~~~

- **DAML Standard Library**: Add ``String`` as a compatibility alias for ``Text``.

Ledger API
~~~~~~~~~~

- **BREAKING** Removed the unused field :ref:`com.digitalasset.ledger.api.v1.ExercisedEvent` from :ref:`com.digitalasset.ledger.api.v1.Event`,
  because a :ref:`com.digitalasset.ledger.api.v1.Transaction` never contains exercised events (only created and archived events): `#960 <https://github.com/digital-asset/daml/issues/960>`_

  This change is *backwards compatible on the transport level*, meaning:

  - new versions of ledger language bindings will work with previous versions of the Sandbox, because the field was never populated
  - previous versions of the ledger language bindings will work with new versions of the Sandbox, as the field was removed without any change in observable behavior

How to migrate:

  - If you check for the presence of :ref:`com.digitalasset.ledger.api.v1.ExercisedEvent` when handling a :ref:`com.digitalasset.ledger.api.v1.Transaction`, you have to remove this code now.

Java Bindings
~~~~~~~~~~~~~

- **BREAKING** Reflect the breaking change of Ledger API in the event class hierarchy:

  - Changed ``data.Event`` from an abstract class to an interface, representing events in a flat transaction.
  - Added interface ``data.TreeEvent``, representing events in a transaction tree.
  - ``data.CreatedEvent`` and ``data.ArchivedEvent`` now implement ``data.Event``.
  - ``data.CreatedEvent`` and ``data.ExercisedEvent`` now implement ``data.TreeEvent``.
  - ``data.TransactionTree#eventsById`` is now ``Map<String, TreeEvent>`` (was previously ``Map<String, Event>``).

How to migrate:

  - If you are processing ``data.TransactionTree`` objects, you need to change the type of the processed events from ``data.Event`` to ``data.TreeEvent``.
  - If you are checking for the presense of exercised events when processing ``data.Transaction`` objects, you can remove that code now.
    It would never have triggered in the first place, as transactions do not contain exercised events.

.. _release-0-12-17:

0.12.17 - 2019-05-10
--------------------

- Making transaction lookups performant so we can handle such requests for large ledgers as well

- **Sandbox**: Transactions with a record time that is after the maximum record time (as provided in the original command) are now properly rejected instead of committed to the ledger.

  See `issue #987 <https://github.com/digital-asset/daml/issues/987>`__ for details.

- **SDK**: The Windows installer no longer requires elevated privileges.

- **DAML Assistant**: The assistant handles network failures gracefully.

.. _release-0-12-16:

0.12.16 - 2019-05-07
--------------------

- **Contract keys**: Fixed two issues related to contract key visibility.

  See `issue #969 <https://github.com/digital-asset/daml/pull/969>`__ and `issue #973 <https://github.com/digital-asset/daml/pull/973>`__ for details.
- **Java Codegen**: Variants with unserializable cases are now accepted.

  See `issue #946 <https://github.com/digital-asset/daml/pull/946>`__ for details.
- **Java Bindings**: ``CreateAndExerciseCommand`` is now properly converted in the Java Bindings data layer.

  See `issue #979 <https://github.com/digital-asset/daml/pull/979>`__ for details.
- **DAML Integration Kit**: Alpha release of the kit for integrating your own ledger with DAML. See the :doc:`DAML Integration Kit docs </daml-integration-kit/index>` for how to try it out.
- **DAML Assistant**: Added a ``quickstart-scala`` DAML Assistant project template.
- **DAML-LF Engine**: If all labels in a record are set, fields no longer need to be ordered.

  See `issue #988 <https://github.com/digital-asset/daml/issues/988>`__ for details.

.. _release-0-12-15:

0.12.15 - 2019-05-06
--------------------

- **Windows support**: Beta release of the Windows SDK.

  To try it out, download the installer from `GitHub releases <https://github.com/digital-asset/daml/releases>`__. The Windows SDK uses the new ``daml`` command-line which will soon also
  become the default on Linux and MacOS.

  Documentation is still in progress, but you can see the :doc:`Migration guide </support/new-assistant>` and the `pull request for the updated documentation <https://github.com/digital-asset/daml/pull/740>`__.
- **DAML Standard Library**: Added ``fromListWith`` and ``merge`` to ``DA.TextMap``.
- **DAML Standard Library**: Deprecated ``DA.Map`` and ``DA.Set``. Use the new ``DA.Next.Map`` and ``DA.Next.Set`` instead.
- **Ledger API**: Added three new methods to the :ref:`CommandService <com.digitalasset.ledger.api.v1.commandservice>`:

  - ``SubmitAndWaitForTransactionId`` returns the transaction ID.
  - ``SubmitAndWaitForTransaction`` returns the transaction.
  - ``SubmitAndWaitForTransactionTree`` returns the transaction tree.

- **Ledger API**: Added field ``transaction_id`` to command completions. This field is only set when a command is successful.
- **DAML  Standard Library**: Added instances of ``Functor``, ``Applicative``, and ``Action`` for ``(->) r`` (the reader monad).

.. _release-0-12-14:

0.12.14 - 2019-05-03
--------------------

- **DAML Standard Library**: The ``id`` function was previously deprecated and has now been removed. Use ``identity`` instead.
- **DAML and Assistant**: The compiler no longer supports DAML-LF 1.0.
- **DAML-LF**: As a new "dev" minor version, writing with ``--target 1.dev`` is now supported by all tools by default.
- **Ledger API**: You can now look up flat transactions with the new TransactionService methods ``GetFlatTransactionByEventId`` and ``GetFlatTransactionById``.

.. _release-0-12-13:

0.12.13 - 2019-05-02
--------------------

- **Sandbox**: Fixed an problem with Postgres of potentially not stopping the transaction stream at required ceiling offset.

  For more details, see `the pull request <https://github.com/digital-asset/daml/pull/802>`__.

.. _release-0-12-12:

0.12.12 - 2019-04-30
--------------------

- **Sandbox**: Added support for using a Postgres database as a back end for the Sandbox, which gives you persistent data storage. To try it out, see :doc:`/tools/sandbox`.
- **DAML Integration Kit**: Added documentation for :doc:`/daml-integration-kit/index`. The docs explain what the DAML Integration Kit is, what state it is in, and how it is going to evolve.
- **DAML Integration Kit**: Released the Ledger API Test Tool. To try it out, see :doc:`/tools/ledger-api-test-tool/index`.
- **DAML-LF**: Removed DAML-LF Dev major version, ``--target dev`` option, and sandbox ``--allow-dev`` option.

  A "1.dev" target will handle the intended "Dev" use cases in a future release.
- **Ledger API**: The list of DAML packages used during interpretation is now included in the produced transaction.
- **Scala**: Source JARs are now released for Scala libraries.
- **DAML  Standard Library**: Renamed ``DA.TextMap.filter`` and ``DA.Map.filter`` to ``filterWithKey``.
- **Contract keys**: Fixed bug releated to visibility and contract keys.

  For details, see `issue #751 <https://github.com/digital-asset/daml/issues/751>`__.
- **Contract keys**: Fixed bug related witness parties in transaction events.

  For details, see `issue #794 <https://github.com/digital-asset/daml/issues/794>`__.

.. _release-0-12-11:

0.12.11 - 2019-04-26
--------------------

- **Node.js Bindings**: The Node.js bindings have been moved to `github.com/digital-asset/daml-js <https://github.com/digital-asset/daml-js>`__.
- **DAML**: Added documentation for flexible controllers. To read about them, see :doc:`/daml/reference/structure`, and for an example, see :doc:`/daml/patterns/multiparty-agreement`.

.. _release-0-12-10:

0.12.10 — 2019-04-25
--------------------

- **DAML-LF**: DAML-LF 1.3 is now the default compilation target for the DAML compiler. This means that contract keys and text maps are now available by default in DAML.

.. _release-0-12-9:

0.12.9 — 2019-04-23
-------------------

- **DAML Standard Library**: Added the ``DA.Math`` library containing exponentiation, logarithms and trig functions
- **Ledger API**: Added ``CreateAndExerciseCommand`` to the Ledger API and DAMLe for creating a contract and exercising a choice on it within the same transaction.

  You can use this to implement "callable updates": functions of type ``Update a`` that can be called from the Ledger API via a contract.
- Publish the participant-state APIs and reference implementations.
- **Sandbox**: Added the ``-s`` option to the CLI to have a shortened version for ``--static-time``.
- **Sandbox**: Change ``--allow-dev`` to be a hidden CLI option, as it's generally not relevant for end users.

.. _release-0-12-7:

0.12.7 — 2019-04-17
-------------------

No user-facing changes.

.. - Fix release pipeline (hopefully)

.. _release-0-12-6:

0.12.6 — 2019-04-16
-------------------

- **Java Bindings**: Removed blocking call inside ``Bot.wire``, which could lead to an application not making progress in certain situations.

.. _release-0-12-5:

0.12.5 — 2019-04-15
-------------------

.. - Fix release pipeline (hopefully)

- **DAML-LF**: The DAML-LF Archive Protobuf definitions are now packaged so that it's possible to use them without mangling the path.

.. _release-0-12-4:

0.12.4 — 2019-04-15
-------------------

- **SDK**: Build artifacts are now released to GitHub.
- **Sandbox**: We now avoid recompiling packages after resetting using the ``ResetService``.
- **Scala**: The compiled ``google.rpc.Status`` is now included in the ``ledger-api-scalapb`` jar.
- **Ledger API**: Fixed critical bug related to the conversion of decimal numbers from Ledger API.

  For details, see `issue #399 <https://github.com/digital-asset/daml/issues/399>`__ and `issue #439 <https://github.com/digital-asset/daml/pull/439>`__.

.. _release-0-12-3:

0.12.3 — 2019-04-12
-------------------

- **SDK**: Fix Navigator and Extractor packaging.

.. _release-0-12-2:

0.12.2 — 2019-04-12
-------------------

- **DAML**: Added flexible controllers and disjunction choices.
- **Sandbox**: Introduced experimental support for using Postgres as a backend. The optional CLI argument for it, ``--jdbcurl``, is still hidden.
- **Node.js Bindings**: Fixed validation for Ledger API timestamp values.
- **Node.js Bindings**: Drop support for identifier names, replacing them with
  separated module and entity names.
- **Node.js Bindings**: Ledger API timestamps and dates are now represented with strings instead of numbers.
- **Node.js Bindings**: Protobuf 64-bit precision integers now use strings instead of numbers, to avoid a loss of precision.
- **Java Codegen**: Added support for DAML TextMap primitive. This is mapped to the ``java.util.Map`` type, with keys restricted to ``java.lang.String`` instances.
- **Java Codegen**: Made log output leaner.
- **Java Codegen**: Added flag for log verbosity: ``-V LEVEL`` or ``--verbosity
  LEVEL``, where ``LEVEL`` is a number between ``0`` (least verbose) and ``4``
  (most verbose).
- **BREAKING - Sandbox and DAMLe**: Remove support for DAML 1.0 packages in the engine, and thus the Sandbox. Note that the SDK has removed support for *compiling* DAML 1.0 months ago.

.. _release-0-12-1:

0.12.1 — 2019-04-04
-------------------

No user-facing changes.

.. - Fix release process

.. _release-0-12-0:

0.12.0 — 2019-04-04
-------------------

- Change in how values are addressed in Navigator's ``frontend-config.js``.

  - Old syntax for accessing values: ``argument.foo.bar``
  - New syntax:

    .. code-block:: javascript

       import { DamlLfValue } from '@da/ui-core';
       // Accessing field 'bar' of field 'foo' of the argument
       DamlLfValue.evalPath(argument, ["foo", "bar"])
       DamlLfValue.toJSON(argument).foo.bar


.. _release-0-11-32:

0.11.32
-------

- **BREAKING CHANGE - DAML standard library**: Removed ``DA.List.split`` function, which was never intended to be exposed and doesn't do what the name suggests.
- **BREAKING CHANGE - Java Bindings**: Removed type parameter for ``DamlList`` and ``DamlOptional`` classes.

  The ``DamlList``, ``DamlOptional``, and ``ContractId`` classes were previously parameterized (i.e ``DamlList[String]``) for consistency with the DAML language. The type parameter has been removed as such type information is not supported by the underlying Ledger API and and therefore the parameterized type couldn’t be checked for correctness.
- **BREAKING CHANGE - Java Bindings**: For all classes in the package ``com.daml.ledger.javaapi.data``, we shortened the names of the conversion methods from long forms like ``fromProtoGeneratedCompletionStreamRequest`` and ``toProtoGeneratedCompletionStreamRequest`` to the much shorter ``fromProto`` and ``toProto``.
- **Navigator**: Added support for Optional and recursive data types.
- **Navigator**: Improved start up performance for big DAML models.
- **BREAKING CHANGE - Navigator**: Refactor the GraphQL API.

  If you're maintaining a modified version of the Navigator frontend, you'll need to adapt all your GraphQL queries to the new API.
- **Navigator**: Fixed an issue where it was not possible to enter contract arguments involving contract IDs.
- **Navigator**: Fixed issues where the console could not read some events or commands from its database.
- **BREAKING CHANGE - DAML**: For the time being, data types with a single data constructor not associated with an argument are not accepted. For example, ``data T = T``.

  To work around this, use ``data T = T {}`` or ``data T = T ()`` (depending on whether you desire ``T`` be interpreted as a product or a sum).

.. _release-0-11-3:

0.11.3 - 2019-02-07
-------------------

- **Navigator**: Fixed display of Date values.
- **Extractor**: Added first version of Extractor with PostgreSQL support.

.. _release-0-11-2:

0.11.2 - 2019-01-31
-------------------

- **Navigator**: Added a terminal-based console interface using SQLite as a backend.
- **Navigator**: Now writes logs to ``./navigator.log`` by default using Logback.
- **DAML Studio**: Significant performance improvements.
- **DAML Studio**: New table view for scenario results.
- **DAML Standard Library**: New type classes.
- **Node.js bindings**: Documentation updated to use version 0.4.0 and DAML 1.2.

.. _release-0-11-1:

0.11.1 - 2019-01-24
-------------------

- **Java Bindings**: Fixed ``Timestamp.fromInstant`` and ``Timestamp.toInstant``.
- **Java Bindings**: Added ``Timestamp.getMicroseconds``.

.. _release-0-11-0:

0.11.0 - 2019-01-17
-------------------

- **Documentation**: :doc:`DAML documentation </daml/reference/index>` and :doc:`examples </examples/examples>` now use DAML 1.2.

  To convert your code to DAML 1.2, see the :doc:`conversion guide </daml//1-2-conversion>`.
- **Documentation**: Added a comprehensive :doc:`quickstart guide </getting-started/quickstart>` that replaces the old "My first project" example.

	As part of this, removed the My first project, IOU and PvP examples.
- **Documentation**: Added a :doc:`guide to building applications against a DA ledger </app-dev/app-arch>`.
- **Documentation**: Updated the :doc:`support and feedback page <support>`.
- **Ledger API**: Version 1.4.0 has support for multi-party subscriptions in the transactions and active contracts services.
- **Ledger API**: Version 1.4.0 supports the verbose field in the transactions and active contracts services.
- **Ledger API**: Version 1.4.0 has full support for transaction trees.
- **Sandbox**: Implements Ledger API version 1.4.0.
- **Java Bindings**: Examples updated to use version 2.5.2 which implements Ledger API version 1.4.0.

.. toctree::
   :hidden:

   /support/new-assistant
