.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------


.. _release-0-13-4:

0.13.4 - 2019-06-19
-------------------

Java Codegen
~~~~~~~~~~~~

- Support generic types (including tuples) as contract keys in codegen.
  See `#1728 <https://github.com/digital-asset/daml/issues/1728>`__.

Ledger API
~~~~~~~~~~

- A new command ``ExerciseByKey`` allows to exercise choices on active contracts referring to them by their key.
  See `#1366 <https://github.com/digital-asset/daml/issues/1366>`__.

Java Bindings
~~~~~~~~~~~~~

- The addition of the ``ExerciseByKey`` to the Ledger API is reflected in the bindings.
  See `#1366 <https://github.com/digital-asset/daml/issues/1366>`__.

Release Procedure
~~~~~~~~~~~~~~~~~

- Fixes to the release procedure. Note: The release to Maven Central was successfully
  performed _manually_ in release 0.13.3. This release should confirm that it will occur
  as part of the CI/CD.
  See `#1745 <https://github.com/digital-asset/daml/issues/1745>`__

DAML Studio
~~~~~~~~~~~

- Closing and reopening scenario results will now show the results
  instead of an empty view.
  See `#1606 <https://github.com/digital-asset/daml/issues/1606>`__.

.. _release-0-13-3:

0.13.3 - 2019-06-18
-------------------

Release Procedure
~~~~~~~~~~~~~~~~~

- Fixes to the release procedure.
  See `#1737 <https://github.com/digital-asset/daml/issues/1737>`__

Java Bindings
~~~~~~~~~~~~~

- The changes for Java Bindings listed for SDK 0.13.2 now only apply to SDK 0.13.3 and later.
  This is due to the partial failure of the release procedure.

Docs
~~~~

- Added :doc:`/daml/intro/0_Intro`

DAML Studio
~~~~~~~~~~~

- The IDE now executes tasks in parallel.

Sandbox
~~~~~~~

- Fixed a bug in migration scripts that could cause databases originally created
  with older versions of the Sandbox to not upgrade schemas properly.
  See `#1682 <https://github.com/digital-asset/daml/issues/1682>`__.

.. _release-0-13-2:

0.13.2 - 2019-06-18
-------------------

Visualizing DAML Contracts
~~~~~~~~~~~~~~~~~~~~~~~~~~

- Added :doc:`Visualizing DAML Contracts </tools/visual>`

Release Procedure
~~~~~~~~~~~~~~~~~

- Fixes to the release procedure.
  See `#1725 <https://github.com/digital-asset/daml/issues/1725>`__

- The changes for Java Bindings listed for SDK 0.13.1 now only apply to SDK 0.13.2 and later.
  This is due to the partial failure of the release procedure.

.. _release-0-13-1:

0.13.1 - 2019-06-17
-------------------

Language
~~~~~~~~

- Add an instance for ``IsParties (Optional Party)``, allowing ``Optional`` values to be used in ``signatory``, ``observer`` and ``maintainer`` clauses.

Java Bindings
~~~~~~~~~~~~~

- Release the Java Bindings to the public Maven Central repository. To move to using the Maven Central repository, remove
  the ``<repository>...</repository>`` and ``<pluginRepository>...</pluginRepository>`` blocks from Maven POM files
  that use version 0.13.1 (or later) of the Java Bindings.
  See `#1205 <https://github.com/digital-asset/daml/issues/1205>`__.

.. _release-0-13-0:

0.13.0 - 2019-06-17
-------------------

SDK
~~~

- This marks the first release that is no longer released for the
  ``da`` assistant. It is still possible to use it to get older SDK
  releases. Take a look at `documentation
  <https://docs.daml.com/tools/assistant.html>`__ for the new ``daml``
  assistant for migration instructions.

Sandbox
~~~~~~~

- Fixed a bug in an internal data structure that broke contract keys.
  See `#1623 <https://github.com/digital-asset/daml/issues/1623>`__.
- Fixed a bug of not closing a resource properly when shutting down the Sandbox.
  See `#1702 <https://github.com/digital-asset/daml/pull/1702>`__.

DAML Studio
~~~~~~~~~~~

- Double the gRPC message limit used for the scenario service. This
  avoids issues on large projects.

Ledger API
~~~~~~~~~~

- Slash (/) is now an allowed character in contract, workflow, application
  and command identifiers.

.. _release-0-12-25:

0.12.25 — 2019-06-13
--------------------

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- Added new CLI flag ``--all-tests`` to the :doc:`Ledger API Test Tool
  </tools/ledger-api-test-tool/index>` to run all default and optional tests.
- Added new CLI flag ``--command-submission-ttl-scale-factor`` to the
  :doc:`Ledger API Test Tool </tools/ledger-api-test-tool/index>`. It scales
  time-to-live of commands sent for ledger processing (captured as Maximum Record
  Time in submitted transactions) for some suites. Useful to tune Maximum Record
  Time depending on the environment and the Ledger implementation under test.
- Fixed various bugs in the daml-on-x ledger api server and index service.

Sandbox
~~~~~~~

- Introduced a new API for party management.
  See `#1312 <https://github.com/digital-asset/daml/issues/1312>`__.

Scala bindings
~~~~~~~~~~~~~~

- New `--root` command-line option for limiting what templates are selected for codegen.
  See `#1210 <https://github.com/digital-asset/daml/pull/1210>`__.

Ledger API
~~~~~~~~~~

- Contract keys are now available for created events from the transaction service.
  See `#1268 <https://github.com/digital-asset/daml/issues/1268>`__.

Java Bindings
~~~~~~~~~~~~~

- The addition of contract keys on created events in the Ledger API is reflected in the bindings.
  See `#1268 <https://github.com/digital-asset/daml/issues/1268>`__.

Java Codegen
~~~~~~~~~~~~

- Contracts decoded from the transaction service now expose their contract key (if defined).
  See `#1268 <https://github.com/digital-asset/daml/issues/1268>`__.

.. _release-0-12-24:

0.12.24 - 2019-06-06
--------------------

DAML Studio
~~~~~~~~~~~

- Fix errors due to unhandled ``$/cancelRequest`` and ``textDocument/willSave``
  requests from showing up in the output tab in VSCode. These errors also caused
  an automatic switch from the problems tab to the output tab which should now
  no longer happen.

- Note that upgrading the VSCode extension requires launching it via
  ``daml studio``. If you launch VSCode directly, you might get issues
  due to an outdated extension.

.. _release-0-12-23:

0.12.23 - 2019-06-05
--------------------

SQL Extractor
~~~~~~~~~~~~~

- 50MiB is no longer hard-coded on extractor input for sandbox or any other server,
  permitting large packages; e.g. pass ``--ledger-api-inbound-message-size-max 62914560``
  to extractor to get a 60MiB limit.
  See `#1520 <https://github.com/digital-asset/daml/pull/1520>`__.
- Improving logging. See `#1518 <https://github.com/digital-asset/daml/pull/1518>`__.

DAML Language
~~~~~~~~~~~~~

- **BREAKING CHANGE**: Contract key maintainers must now explicitly be computed from the contract key using the implicit ``key`` variable. For instance, if you have ``key (bank, accountId) : (Party, Text)`` and want ``bank`` to be the maintainer, you have to write ``maintainer key._1`` (before, you could write ``maintainer bank``).

DAML Compiler
~~~~~~~~~~~~~

- **BREAKING CHANGE**: Drop support for DAML-LF 1.3. Compiling to DAML-LF 1.4 should work without any code changes, although we highly recommend not specifying a target DAML-LF version at all. (The ledger server still supports DAML-LF 1.3.)

- Fix initialization of package-db for non-default DAML-LF versions.
  This fixes issues when using "daml build --target 1.3" (or other target versions).

DAML Standard Library
~~~~~~~~~~~~~~~~~~~~~

- Add ``enumerate`` function.

Navigator
~~~~~~~~~~~

- Fixed a regression where Navigator console was not able to inspect contracts and events.
  See `#1454 <https://github.com/digital-asset/daml/issues/1454>`__.
- 50MiB is no longer hard-coded on extractor input for sandbox or any other server,
  permitting large packages; e.g. pass ``--ledger-api-inbound-message-size-max 62914560``
  to extractor to get a 60MiB limit.
  See `#1520 <https://github.com/digital-asset/daml/pull/1520>`__.


Sandbox
~~~~~~~

- Added recovery around failing ledger entry persistence queries using Postgres. See `#1505 <https://github.com/digital-asset/daml/pull/1505>`__.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- The :doc:`Ledger API Test Tool </tools/ledger-api-test-tool/index>` can now optionally run ``TransactionServiceIT`` as part of the conformance tests.
  This means you need to load additional ``.dar`` files into the ledger under test. Please refer to the updated instructions in the :doc:`documentation </tools/ledger-api-test-tool/index>`.
- Added new CLI options to the :doc:`Ledger API Test Tool </tools/ledger-api-test-tool/index>`:

  - ``--list`` prints all available tests to the console
  - ``--include`` takes a comma-separated list of test names that should be run
  - ``--exclude`` takes a comma-separated list of test names that should not be run

0.12.22 - 2019-05-29
--------------------

DAML Studio
~~~~~~~~~~~

- Fixed a bug where type check errors would persist if there was a subsequent parse error.

DAML Compiler
~~~~~~~~~~~~~

- **BREAKING CHANGE**: Drop support for DAML-LF 1.2. Compiling to DAML-LF 1.3 should work without any code changes, although we highly recommend not specifying a target DAML-LF version at all.
- **BREAKING CHANGE**: By default ``damlc test`` must be executed in a project and will test the whole project. Testing individual files, potentially outside a project, requires passing the new ``--files`` flag.

DAML-LF
~~~~~~~

- The Syntax of party literals is relaxed by allowing the character colon. Concretely those literals must match the
  regular expression ``[a-zA-Z0-9:\-_ ]+`` instead of ``[a-zA-Z0-9\-_ ]+`` previously.
  See `#1467 <https://github.com/digital-asset/daml/pull/1467>`__.

SQL Extractor
~~~~~~~~~~~~~

- The extractor ``--party`` option may now specify multiple parties, separated by commas;
  e.g. instead of ``--party Bob`` you can say ``--party Bob,Bar,Baz`` and get the contracts
  for all three parties in the database.
  See `#1360 <https://github.com/digital-asset/daml/pull/1360>`__.

- The extractor ``--templates`` option to specify template IDs in the format:
  ``<module1>:<entity1>,<module2>:<entity2>``. If not provided, extractor subscribes to all available templates.
  See `#1352 <https://github.com/digital-asset/daml/issues/1352>`__.

Sandbox
~~~~~~~

- Fixed a bug in the SQL backend that caused transactions with a fetch node referencing a contract created in the same transaction to be rejected.
  See `issue #1435 <https://github.com/digital-asset/daml/issues/1435>`__.

0.12.21 - 2019-05-28
--------------------

DAML Assistant
~~~~~~~~~~~~~~

- The ``exposed-modules`` field in ``daml.yaml`` is now optional. If it is
  not specified, all modules in the project are exposed.
  See `#1328 <https://github.com/digital-asset/daml/issues/1328>`_.

- You can now see all available versions with ``daml version`` using the ``--all`` flag.

DAML Compiler
~~~~~~~~~~~~~

- **BREAKING CHANGE**: Drop support for DAML-LF 1.1. Compiling to DAML-LF 1.2 should work without any code changes, although we highly recommend not specifying a target DAML-LF version at all.

- Make DAML-LF 1.5 the default version produced by the compiler.

DAML Standard Library
~~~~~~~~~~~~~~~~~~~~~

- ``parseInt`` and ``parseDecimal`` now work at more extremes of values and accept leading plus signs.

DAML-LF
~~~~~~~

- Add new version 1.5. See `DAML-LF 1 specification <https://github.com/digital-asset/daml/blob/master/daml-lf/spec/daml-lf-1.rst#version-1-5>`_ for details.

Ledger
~~~~~~

- **BREAKING CHANGE**: The string fields ``application_id``, ``command_id``, ``ledger_id``, and ``workflow_id``
  in Ledger API commands must now match the regular expression  ``[A-Za-z0-9\._:\-#]{1,255}``. Those fields
  were unrestricted UTF-8 strings in previous versions.
  See `#398 <https://github.com/digital-asset/daml/issues/398>`__.

0.12.20 - 2019-05-23
--------------------

Sandbox
~~~~~~~

- **Contract keys**: Support arbitrary key expressions (this was accidentally omitted from 0.12.19).

0.12.19 - 2019-05-22
--------------------

Ledger
~~~~~~

- Transaction filters in `GetTransactionsRequest` without any party are now rejected with `INVALID_ARGUMENT` instead of yielding an empty stream

  See `#1250 <https://github.com/digital-asset/daml/issues/1250>`__ for details.

DAML
~~~~

- **Contract keys**: The syntactic restriction on contract keys has been removed. They can be arbitray expressions now.

DAML-LF
~~~~~~~

- Add new version 1.4 and make it the default version produced by ``damlc``. It removes the syntactic restriction on contract keys.

Java Bindings
~~~~~~~~~~~~~

- **Bots**: A class called LedgerTestView was added to make bot unit testing possible

DAML
~~~~

- **BREAKING CHANGE - Syntax**: Records with empty update blocks, e.g. ``foo with``, is now an error (the fact it was ever accepted was a bug).

- **BREAKING CHANGE - Contract Keys**: Before, maintainers were incorrectly not checked to be a subset of the signatories, now they are. See `issue #1123 <https://github.com/digital-asset/daml/issues/1123>`__

Sandbox
~~~~~~~

- When loading a scenario with ``--scenario``, the sandbox no longer compiles packages twice, see
  `issue #1238 <https://github.com/digital-asset/daml/issues/1238>`__.
- When starting the sandbox, you can now choose to have it load all the ``.dar`` packages immediately
  with the ``--eager-package-loading`` flag. The default behavior is to load the packages only when
  a command requires them, which causes a delay for the first command that requires a yet-to-be-compiled
  package.
  See `issue #1230 <https://github.com/digital-asset/daml/issues/1230>`__.

SDK tools
~~~~~~~~~

- The Windows installer is now signed. You might still see Windows
  defender warnings for some time but the publisher should now show
  "Digital Asset Holdings, LLC".

.. _release-0-12-18:

0.12.18 - 2019-05-20
--------------------

Documentation
~~~~~~~~~~~~~

- Removed unnecessary dependency in the quickstart-java example project.
- Removed the *Configure Maven* section from the installation instructions. This step is not needed anymore.

SDK tools
~~~~~~~~~

- **DAML Assistant**: We've built a new and improved version of the SDK assistant, replacing ``da`` commands with ``daml`` commands. The documentation is updated to use the new assistant in this release.

  For a full guide to what's changed and how to migrate, see :doc:`/support/new-assistant`. To read about how to use the new ``daml`` Assistant, see :doc:`/tools/assistant`.

DAML
~~~~

- **BREAKING CHANGE - DAML Compiler**: It is now an error to omit method bodies in class ``instance`` s if the method
  has no default. Almost all instances of such behaviour were an error - add in a suitable definition.

- **Contract keys**: We've added documentation for contract keys, a way of specifying a primary key for contract instances. For information about how to use them, see :doc:`/daml/reference/contract-keys`.

- **BREAKING CHANGE - DAML Standard Library**: Moved the ``Tuple`` and ``Either`` types to ``daml-prim:DA.Types``
  rather than exposing internal locations.

  How to migrate:

  - You don't need to change DAML code as a result of this change.
  - People using the Java/Scala codegen need to replace ``import ghc.tuple.*`` or ``import da.internal.prelude.*`` with ``import da.types.*``.
  - People using the Ledger API directly need to replace ``GHC.Tuple`` and ``DA.Internal.Prelude`` with ``DA.Types``.

- **BREAKING CHANGE - DAML Standard Library**: Don't expose the ``TextMap`` type via the ``Prelude`` anymore.

  How to migrate: Always import ``DA.TextMap`` when you want to use the ``TextMap`` type.

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

- Added the :ref:`agreement text <daml-ref-agreements>` as a new field ``agreement_text`` to the ``CreatedEvent`` message. This means you now have access to the agreement text of contracts via the Ledger API.
  The type of this field is ``google.protobuf.StringValue`` to properly reflect the optionality on the wire for full backwards compatibility.
  See Google's `wrappers.proto <https://github.com/protocolbuffers/protobuf/blob/b4f193788c9f0f05d7e0879ea96cd738630e5d51/src/google/protobuf/wrappers.proto#L31-L34>`__ for more information about ``StringValue``.

  See `#1110 <https://github.com/digital-asset/daml/issues/1110>`__ for details.

- Fixed: the `CommandService.SubmitAndWait` endpoint no longer rejects commands without a workflow identifier.

  See `#572 <https://github.com/digital-asset/daml/issues/572>`__ for details.

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

- **Java Codegen**: You can now call a method to get a ``CreateAndExerciseCommand`` for each choice, for example:

  .. code-block:: java

     CreateAndExerciseCommand cmd = new MyTemplate(owner, someText).createAndExerciseAccept(42L);

  In this case ``MyTemplate`` is a DAML template with a choice ``Accept`` and the resulting command will create a contract and exercise the ``Accept`` choice within the same transaction.

  See `issue #1092 <https://github.com/digital-asset/daml/issues/1092>`__ for details.

- Added :ref:`agreement text <daml-ref-agreements>` of contracts: `#1110 <https://github.com/digital-asset/daml/issues/1110>`__

  - **Java Bindings**

    - Added field ``Optional<String> agreementText`` to ``data.CreatedEvent``, to reflect the change in Ledger API.

  - **Java Codegen**

    - Added generated field ``Optional<String> TemplateName.Contract#agreementText``.
    - Added generated static method ``TemplateName.Contract.fromCreatedEvent(CreatedEvent)``.
      This is the preferred method to use for converting a ``CreatedEvent`` into a ``Contract``.
    - Added generated static method ``TemplateName.Contract.fromIdAndRecord(String, Record, Optional<String>)``.
      This method is useful for setting up tests, when you want to convert a ``Record`` into a contract without having to create a ``CreatedEvent`` first.
    - Deprecated generated static method ``TemplateName.Contract.fromIdAndRecord(String, Record)`` in favor of the new static methods in the generated ``Contract`` classes.
    - Changed the generated :ref:`decoder utility class <daml-codegen-java-decoder-class>` to use the new ``fromCreatedEvent`` method.
    - **BREAKING** Changed the return type of the ``getDecoder`` method in the generated decoder utility class from ``Optional<BiFunction<String, Record, Contract>>`` to ``Optional<Function<CreatedEvent, Contract>>``.

  How to migrate:

  - If you are manually constructing instances of ``data.CreatedEvent`` (for example, for testing), you need to add an ``Optional<String>`` value as constructor parameter for the ``agreementText`` field.
  - You should change all calls to ``Contract.fromIdAndRecord`` to ``Contract.fromCreatedEvent``.

    .. code-block:: java

        // BEFORE
        CreatedEvent event = ...;
        Iou.Contract contract = Iou.Contract.fromIdAndRecord(event.getContractId(), event.getArguments()));

        // AFTER
        CreatedEvent event = ...;
        Iou.Contract contract = Iou.Contract.fromCreatedEvent(event);

  - Pass the ``data.CreatedEvent`` directly to the function returned by the decoder's ``getDecoder`` method.
    If you are using the decoder utility class method ``fromCreatedEvent``, you don't need to change anything.

    .. code-block:: java

        CreatedEvent event = ...;
        // BEFORE
        Optional<BiFunction<String, Record, Contract>> decoder = MyDecoderUtility.getDecoder(MyTemplate.TEMPLATE_ID);
        if (decoder.isPresent()) {
            return decoder.get().apply(event.getContractId(), event.getArguments();
        }

        // AFTER
        Optional<Function<CreatedEvent, Contract>> decoder = MyDecoderUtility.getDecoder(MyTemplate.TEMPLATE_ID);
        if (decoder.isPresent()) {
            return decoder.get().apply(event);
        }

Scala Bindings
~~~~~~~~~~~~~~

- **BREAKING** You can now access the :ref:`agreement text <daml-ref-agreements>` of a contract with the new field ``Contract#agreementText: Option[String]``.

  How to migrate:

  - If you are pattern matching on ``com.digitalasset.ledger.client.binding.Contract``, you need to add a match clause for the added field.
  - If you are constructing ``com.digitalasset.ledger.client.binding.Contract`` values, for example for tests, you need to add a constructor parameter for the agreement text.

- ``CreateAndExercise`` support via ``createAnd`` method, e.g. ``MyTemplate(owner, someText).createAnd.exerciseAccept(controller, 42)``.
  See `issue #1092 <https://github.com/digital-asset/daml/issues/1092>`__ for more information.

Ledger
~~~~~~

- Renamed ``--jdbcurl`` to ``--sql-backend-jdbcurl``. Left ``--jdbcurl`` in place for backwards compat.
- Fixed issue when loading scenarios making use of ``pass`` into the sandbox, see
  `#1079 <https://github.com/digital-asset/daml/pull/1079>`_.
- Fixed issue when loading scenarios that involve contract divulgence, see
  `#1166 <https://github.com/digital-asset/daml/issues/1166>`_.
- Contract visibility is now properly checked when looking up contracts in the SQL backend, see
  `#784 <https://github.com/digital-asset/daml/issues/784>`_.
- The sandbox now exposes the :ref:`agreement text <daml-ref-agreements>` of contracts in :ref:`CreatedEvents <com.digitalasset.ledger.api.v1.CreatedEvent>`. See `#1110 <https://github.com/digital-asset/daml/issues/1110>`__

Navigator
~~~~~~~~~

- Non-empty :ref:`agreement texts <daml-ref-agreements>` are now shown on the contract page above the section ``Contract details``, see `#1110 <https://github.com/digital-asset/daml/issues/1110>`__

SQL Extractor
~~~~~~~~~~~~~

- **BREAKING** In JSON content, dates and timestamps are formatted like
  ``"2020-02-22"`` and ``"2020-02-22T12:13:14Z"`` rather than UNIX epoch offsets like
  ``18314`` or ``1582373594000000``. See `#1174 <https://github.com/digital-asset/daml/issues/1174>`__
  for more details.

.. _release-0-12-17:

0.12.17 - 2019-05-10
--------------------

- Making transaction lookups performant so we can handle such requests for large ledgers as well

- **Sandbox**: Transactions with a record time that is after the maximum record time (as provided in the original command) are now properly rejected instead of committed to the ledger.

  See `issue #987 <https://github.com/digital-asset/daml/issues/987>`__ for details.

- **SDK**: The Windows installer no longer requires elevated privileges.

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
- Beta release of the Windows SDK:
  You can download the installer from
  `GitHub releases <https://github.com/digital-asset/daml/releases>`_.
  The Windows SDK ships with the new `daml` installer which will soon also
  become the default on Linux and MacOS. Documentation is still in progress,
  take a look at the `Migration guide <https://github.com/digital-asset/daml/pull/768>`_
  and the `updated documentation <https://github.com/digital-asset/daml/pull/740>`_.
- Add ``fromListWith`` and ``merge`` to ``DA.TextMap``.
- Release Javadoc artifacts as part of the SDK. See more `here https://github.com/digital-asset/daml/pull/896`
- Add ``DA.Next.Map`` and ``DA.Next.Set`` and deprecate ``DA.Map`` and ``DA.Set`` in favor of those.
- Ledger API: Added three new methods to :ref:`CommandService <com.digitalasset.ledger.api.v1.commandservice>`:

  - ``SubmitAndWaitForTransactionId`` returns the transaction id.
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
