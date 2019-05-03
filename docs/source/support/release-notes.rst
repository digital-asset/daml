.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- Add ``DA.Next.Map`` and ``DA.Next.Set`` and deprecate ``DA.Map`` and ``DA.Set`` in favor of those.
- Ledger API: Added three new methods to :ref:`CommandService <com.digitalasset.ledger.api.v1.commandservice>`:

  - ``SubmitAndWaitForTransactionId`` returns the transaction id.
  - ``SubmitAndWaitForTransaction`` returns the transaction.
  - ``SubmitAndWaitForTransactionTree`` returns the transaction tree.

- Ledger API: Added field ``transaction_id`` to command completions. It is only set by the ledger
  with the id of the transaction for a successful command.
- Add instances of ``Functor``/``Applicative``/``Action`` for ``(->) r`` (aka the reader monad)

0.12.14 - 2019-05-03
--------------------

- Beta release of the Windows SDK:
  You can download the installer from
  `GitHub releases <https://github.com/digital-asset/daml/releases>`_.
  The Windows SDK ships with the new `daml` installer which will soon also
  become the default on Linux and MacOS. Documentation is still in progress,
  take a look at the `Migration guide <https://github.com/digital-asset/daml/pull/768>`_
  and the `updated documentation <https://github.com/digital-asset/daml/pull/740>`_.
- Delete the `id` function, use `identity` instead.
- Drop support for DAML-LF 1.0 from compiler
- DAML-LF "dev" minor version. Write with ``--target 1.dev``, supported by all tools by
  default.
- Ledger API: You can now look up flat transactions with the new TransactionService methods
  ``GetFlatTransactionByEventId`` and ``GetFlatTransactionById``.

0.12.13 - 2019-05-02
--------------------

- Fix an issue with Postgres of potentially not stopping the transaction stream at required ceiling offset. See more `here <https://github.com/digital-asset/daml/pull/802>`

0.12.12 - 2019-04-30
--------------------

- Add support for using a Postgres database as a back end for the Sandbox, which gives you persistent data storage. See more at: https://docs.daml.com/tools/sandbox.html
- Add documentation for :doc:`/daml-integration-kit/index`, which explains
  what the DAML Integration Kit is, what state it is in, and how it is going
  to evolve.
- Add Ledger API Test Tool, see :doc:`Ledger API Test Tool
  </tools/ledger-api-test-tool/index>`.
- Remove DAML-LF Dev major version, ``--target dev`` option, and sandbox ``--allow-dev``
  option.  A "1.dev" target will handle the intended "Dev" use cases in a future release.
- Include list of DAML packages used during interpretation in the produced transaction.
- Release source jars for scala libraries.
- Rename ``DA.TextMap.filter`` and ``DA.Map.filter`` to ``filterWithKey``.
- Fix bug releated to visibility and contract keys -- see
  `#751 <https://github.com/digital-asset/daml/issues/751>`.
- Fix bug related witness parties in transaction events -- see
  `#794 <https://github.com/digital-asset/daml/issues/794>`.

0.12.11 - 2019-04-26
--------------------

- Node.js bindings have been moved `here <https://github.com/digital-asset/daml-js>``
- Add documentation for flexible controllers.

0.12.10 — 2019-04-25
--------------------

- Make DAML-LF 1.3 the default compilation target for the DAML compiler. This means that
  contract keys and text maps are now available by default in DAML.

0.12.9 — 2019-04-23
-------------------

- Addition of ``DA.Math`` library containing exponentiation, logarithms and trig functions
- Add CreateAndExerciseCommand to Ledger API and DAMLe for creating a contract
  and exercising a choice on it within the same transaction. This can be used to
  implement "callable updates" (aka functions of type ``Update a`` that can be
  called from the Ledger API via a contract).
- Publish the participant-state APIs and reference implementations.
- Add `-s` option to Sandbox CLI to have a shortened version for `--static-time` as well
- Change `--allow-dev` to be a hidden CLI option in Sandbox

0.12.7 — 2019-04-17
-------------------

- Fix release pipeline (hopefully)

0.12.6 — 2019-04-16
-------------------

- RxJava Bindings: remove blocking call inside Bot.wire, which could lead to an
  application not making progress in certain situations.

0.12.5 — 2019-04-15
-------------------

- Fix release pipeline (hopefully)
- DAML-LF Archive packaging: the DAML-LF Archive Protobuf definitions are now
  packaged so that it's possible to use them without mangling the path.

0.12.4 — 2019-04-15
-------------------

- Release build artifacts to GitHub
- Avoid recompiling packages after resetting the Sandbox via the ResetService.
- Include compiled ``google.rpc.Status`` in the ``ledger-api-scalapb`` jar.
- Fix critical bug related to the conversion of decimal numbers from Ledger API
  (see `#399 <https://github.com/digital-asset/daml/issues/399>` and
  `#439 <https://github.com/digital-asset/daml/pull/439>`).

0.12.3 — 2019-04-12
-------------------

- Fix navigator and extractor packaging in the SDK.

0.12.2 — 2019-04-12
-------------------

- Add flexible controllers and disjunction choices to DAML.
- Introduce experimental support for using Postgres as a backend for the
  Sandbox. The optional CLI argument for it named ``--jdbcurl`` is still
  hidden.
- Node.js Bindings: fix validation for Ledger API timestamp values.
- Node.js Bindings: drop support for identifier names, replacing them with
  separated module and entity names.
- Node.js Bindings: use strings instead of numbers to represent Ledger API
  timestamps and dates.
- Node.js Bindings: use strings instead of numbers to represent Protobuf 64-bit
  precision integers to avoid a loss of precision.
- Java Codegen: support DAML TextMap primitive which is mapped to
  ``java.util.Map`` type with keys restricted to ``java.lang.String``
  instances.
- Java Codegen: leaner log output.
- Java Codegen: add flag for log verbosity: ``-V LEVEL`` or ``--verbosity
  LEVEL``, where ``LEVEL`` is a number between ``0`` (least verbose) and ``4``
  (most verbose).
- **BREAKING** Remove support for DAML 1.0 packages in the engine, and thus the
  sandbox. Note that the SDK has removed support for _compiling_ DAML 1.0
  months ago.

0.12.1 — 2019-04-04
-------------------

- Fix release process

0.12.0 — 2019-04-04
-------------------

- Change in how values are addressed in Navigator's `frontend-config.js`.

  - Old syntax for accessing values: `argument.foo.bar`
  - New syntax:

    .. code-block:: javascript

       import { DamlLfValue } from '@da/ui-core';
       // Accessing field 'bar' of field 'foo' of the argument
       DamlLfValue.evalPath(argument, ["foo", "bar"])
       DamlLfValue.toJSON(argument).foo.bar


0.11.32
-------

- DAML standard library (breaking change): Removed ``DA.List.split`` function, which was never intended to be
  exposed and doesn't do what the name suggests.
- Java Bindings (breaking change): Removed type parameter for ``DamlList`` and ``DamlOptional`` classes.
  The ``DamlList``, ``DamlOptional``, and ``ContractId`` classes were previously parameterized (i.e ``DamlList[String]``)
  for consistency with the DAML language. The type parameter has been removed as such type information
  is not supported by the underlying Ledger API and and therefore the parameterized type couldn’t be
  checked for correctness.
- Java Bindings (breaking change): For all classes in the package ``com.daml.ledger.javaapi.data``, we shortened
  the names of the conversion methods from long forms like ``fromProtoGeneratedCompletionStreamRequest`` and
  ``toProtoGeneratedCompletionStreamRequest`` to the much shorter ``fromProto`` and ``toProto``.
- Navigator: Add support for Optional and recursive data types.
- Navigator: Improve start up performance for big DAML models.
- Navigator (breaking change): Refactor the GraphQL API. If you're maintaining a modified version of
  the Navigator frontend, you'll need to adapt all your GraphQL queries to the new API.
- Navigator: Fix an issue where it was not possible to enter contract arguments involving contract IDs.
- Navigator: Fix issues where the console could not read some events or commands from its database.
- DAML syntax (breaking change) : For the time being, datatypes with a single data constructor not associated with an argument are not accepted. For example ``data T = T``. To workaround this use ``data T = T {}`` or ``data T = T ()`` (depending on whether you desire ``T`` be interpreted as a product or a sum).

0.11.3
------

*Released on 2019-02-07*

Changes:

- Navigator: Fix display of Date values.
- Extractor: Add first version of Extractor with PostgreSQL support.

0.11.2
------

*Released on 2019-01-31*

Changes:

- Navigator: Add a terminal-based console interface using SQLite as a backend.
- Navigator: Now writes logs to ./navigator.log by default using Logback.
- DAML Studio: Significant performance improvements.
- DAML Studio: New table view for scenario results.
- DAML Standard Library: New type classes.
- Node.js bindings: Documentation updated to use version 0.4.0 and DAML 1.2.

0.11.1
------

*Released on 2019-01-24*

Changes:

- Java Bindings: Fixed Timestamp.fromInstant and Timestamp.toInstant.
- Java Bindings: Added Timestamp.getMicroseconds.

0.11.0
------

*Released on 2019-01-17*

Changes:

- Documentation: :doc:`DAML documentation </daml/reference/index>` and :doc:`examples </examples/examples>` now use DAML 1.2.

  To convert your code to DAML 1.2, see the :doc:`conversion guide </daml//1-2-conversion>`.
- Documentation: Added a comprehensive :doc:`quickstart guide </getting-started/quickstart>` that replaces the old "My first project" example.

	As part of this, removed the My first project, IOU and PvP examples.
- Documentation: Added a :doc:`guide to building applications against a DA ledger </app-dev/app-arch>`.
- Documentation: Updated the :doc:`support and feedback page <support>`.

- Ledger API: Version 1.4.0 has support for multi-party subscriptions in the transactions and active contracts services.
- Ledger API: Version 1.4.0 supports the verbose field in the transactions and active contracts services.
- Ledger API: Version 1.4.0 has full support for transaction trees.
- Sandbox: Implements Ledger API version 1.4.0.
- Java Bindings: Examples updated to use version 2.5.2 which implements Ledger API version 1.4.0.

.. - TODO: add changes here
