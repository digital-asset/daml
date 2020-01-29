.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

.. _release-0-13-47:

0.13.47 - 2020-01-29
--------------------

DAML Compiler
~~~~~~~~~~~~~

- ``damlc test`` now initializes the packagedb automatically which means that
  it will work on projects that declare custom ``dependencies`` in
  ``daml.yaml`` without having to call ``damlc init`` first.
- Choices marked explicitly as ``preconsuming`` are now equivalent to a
  ``nonconsuming`` choice that calls ``archive self`` at the beginning.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- The simplified kvutils API now uses ``com.digitalasset.resources`` to manage
  acquiring and releasing resources instead of ``Closeable``.

DAML Standard Library
~~~~~~~~~~~~~~~~~~~~~

- Add ``CanAbort`` instance for ``Either Text``.

DAML Studio
~~~~~~~~~~~

- Support all ``build-options`` supported by ``daml build``.

Sandbox
~~~~~~~

- On initialization error, report the error correctly and exit with a status
  code of 1. Previously, the program would hang indefinitely. (This regression
  was introduced in v0.13.41.)
- Upgrade the Flyway database migrations library from v5 to v6.

DAML Triggers - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- DAML triggers can now be tested in scenarios. Specifically, a trigger's
  ``rule`` can be executed in a scenario and assertions performed on the
  emitted commands.


.. _release-0-13-46:

0.13.46 - 2020-01-22
--------------------

Sandbox
~~~~~~~

- The sandbox uses a new payload format for authentication tokens (JWTs). The old format is
  deprecated, but still works.

JSON API
~~~~~~~~

- The HTTP JSON API now uses the same payload format for authentication tokens as the sandbox. The
  old format is deprecated, but still works.

DAML Studio
~~~~~~~~~~~

- Scenarios with unserializable result types no longer crash the scenario service.

.. _release-0-13-45:

0.13.45 - 2020-01-22
--------------------

Sandbox
~~~~~~~

- Metrics are now namespaced by ``"daml"`` and their names have been
  standardized to snake_case.

DAML-LF
~~~~~~~

- Prohibit contract IDs in contract keys completely. Previously, creating keys containing absolute (but not relative) contract IDs was allowed, but ``lookupByKey`` on such a key would crash.

DAML Compiler
~~~~~~~~~~~~~

- Added a ``--drop-orphan-instances`` flag in ``daml damlc docs``.
- The modification times in a DAR are now fixed to a
  given value which makes the output of ``daml build`` deterministic
  in single-threaded mode (which is the default).

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~

- Support Exercise by Key. See `issue #4099 <https://github.com/digital-asset/daml/issues/4099>`__.
- Response format in ``searchForever`` changed to be more like ``exercise``.
  See `issue #4072 <https://github.com/digital-asset/daml/issues/4072>`__.
- In 'search' endpoint arguments, %templates is now templateIds.
  Additionally, all contract query fields must occur under 'query'.
  See `issue #3450 <https://github.com/digital-asset/daml/issues/3450>`__.

Indexer
~~~~~~~

- Potentially fix a bug when recovering from failure.

DAML Standard Library
~~~~~~~~~~~~~~~~~~~~~

- The ``Template``, ``Choice``, and
  ``TemplateKey`` typeclasses have been split up into many small typeclasses
  to improve forward compatibility of DAML models. ``Template``,
  ``Choice`` and ``TemplateKey`` constraints can still be used as before.

.. _release-0-13-44:

0.13.44 - 2020-01-17
--------------------

DAML Studio
~~~~~~~~~~~

- Fix a bug introduced in 0.13.43 that caused DAML studio to stop responding after
  code completions were requested.

Ledger API Server
~~~~~~~~~~~~~~~~~

- Publish the resource management code as a library
  under ``com.digitalasset:resources``.

Ledger API Authorization
~~~~~~~~~~~~~~~~~~~~~~~~

- Support EC256 algorithm for JWT rather than EC512

JSON API Experimental
~~~~~~~~~~~~~~~~~~~~~

- WebSocket contract search at ``/contracts/searchForever``.
  See `issue #3936 <https://github.com/digital-asset/daml/pull/3936>`_.

.. _release-0-13-43:

0.13.43 - 2020-01-15
--------------------

DAML Compiler
~~~~~~~~~~~~~

- The ``build-options`` field from ``daml.yaml`` is now also respected when
  ``--project-root`` is used.

DAML SDK
~~~~~~~~

- Docker images for this release and releases in the future are built using
  the Dockerfile of the corresponding git tag and are therefore stable.
  Previously, they were updated whenever the Dockerfile changed.

Ledger API Server
~~~~~~~~~~~~~~~~~

- **BREAKING CHANGE** ``lookupByKey`` now requires the submitter to be a
  stakeholder on the referenced contract.
  See `issue #2311 <https://github.com/digital-asset/daml/issues/2311>`_
  and `issue #3543 <https://github.com/digital-asset/daml/issues/3543>`_.
- Metrics: Update dropwizard to version 4.1.2.
- Authorization: Support elliptic curve algorithm for JWT verification.

Sandbox
~~~~~~~

- Allow ``submitMustFail`` in scenarios used for sandbox initialization.
- Loosen database schema to allow persistence of transaction ledger entries
  where no submitter info is present (typically when the submitter is hosted
  by another participant node).
- DAML trace logs (trace, traceRaw, traceId) are now logged via the regular
  logging system (slf4j+logback) at interpretation time via the logger
  ``daml.tracelog`` at DEBUG level.
- Fix bug that can cause the transaction stream to not terminate.
  See `issue #3984 <https://github.com/digital-asset/daml/issues/3984>`__.

DAML Triggers - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- You can now configure a heartbeat message to be sent at a regular time interval.

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~
- The ``/contracts/search`` endpoint reports unresolved template IDs as warnings.
  See `issue #3771 <https://github.com/digital-asset/daml/issues/3771>`_.
- Use JSON string to encode template IDs. Use colon (``:``) to separate parts of the ID.
  The request format, with optional package ID:
  - ``"<module>:<entity>"``
  - ``"<package ID>:<module>:<entity>"``
  The response always contains fully qualified template ID in the format:
  - ``"<package ID>:<module>:<entity>"``
  See `issue #3647 <https://github.com/digital-asset/daml/issues/3647>`_.
- Align ``contract`` table with ``domain.ActiveContract`` class.
  The database schema has changed, if using ``--query-store-jdbc-config``,
  you must rebuild the database by adding ``,createSchema=true``.
  See `issue #3754 <https://github.com/digital-asset/daml/issues/3754>`_.
- The ``witnessParties`` field is removed from all JSON responses.


.. _release-0-13-42:

0.13.42 - 2020-01-08
--------------------

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~

- Rename ``argument`` in active contract to ``payload``. See #3826.
- Change variant JSON encoding. The new format is ``{ tag: data-constructor, value: argument }``.
  For example, if we have: ``data Foo = Bar Int | Baz``, these are all valid JSON encodings for
  values of type Foo:
  - ``{"tag": "Bar", "value": 42}``
  - ``{"tag": "Baz", "value": {}}``
  See #3622
- Fix ``/contracts/lookup`` find by contract key.
- Fix ``/command/exercise`` to support any LF type as a choice argument.
  See #3390


DAML Compiler
~~~~~~~~~~~~~

- Move more types from daml-stdlib to standalone LF packages. The module names for the types have
  also changed slightly. This only matters over the Ledger API when you specify the module name
  explicitly. In DAML you should continue to use the existing module names.

  - The types from ``DA.Semigroup`` are now in a separate package under ``DA.Semigroup.Types``.
  - The types from ``DA.Monoid`` are now in a separate package under ``DA.Monoid.Types``.
  - The types from ``DA.Time`` are now in a separate package under ``DA.Time.Types``.
  - The types from ``DA.Validation`` are now in a separate package under ``DA.Validation.Types``.
  - The types from ``DA.Logic`` are now in a separate package under ``DA.Logic.Types``.
  - The types from ``DA.Date`` are now in a separate package under ``DA.Date.Types``.
  - The ``Down`` type from ``DA.Internal.Prelude`` is now in a separate package under ``DA.Internal.Down``.


DAML SDK
~~~~~~~~

- ``daml damlc docs`` now accepts a ``--exclude-instances`` option to exclude unwanted instance docs
  by class name.

DAML-ON-X-SERVER
~~~~~~~~~~~~~~~~

- Made ledger api server to bind to localhost by default instead to the public
  interface for security reasons.

DAML Assistant
~~~~~~~~~~~~~~

- Bash completions for the DAML assistant are now available via ``daml install``. These will be
  installed automatically on Linux and Mac. If you use bash and have bash completions installed,
  these bash completions let you use the tab key to autocomplete many DAML Assistant commands, such
  as ``daml install`` and ``daml version``.

- Zsh completions for the DAML Assistant are now installed as part of ``daml install``. To activate
  them you need to add ``~/.daml/zsh`` to your ``$fpath``, e.g., by adding ``fpath=(~/.daml/zsh
  $fpath)`` to the beginning of your ``~/.zshrc`` before you call ``compinit``.

DAML Script - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~

- Allow running DAML scripts as test-cases.  Executing ``daml test-script --dar mydar.dar`` will
  execute all definitions matching the type ``Script a`` as test-cases.
  See `#3687 <https://github.com/digital-asset/daml/issues/3687>`__.

Reference v2
~~~~~~~~~~~~

- On an exception, shut down everything and crash.
  Previously, the server would stay in a half-running state.

.. _release-0-13-41:

0.13.41 - 2019-12-18
--------------------

DAML Ledger Integration Kit
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Move to asyncronous package management service (#3806)
- Fix indexer crash on duplicate submission.  See #3847
- Standardize and cleanup metric names to use underscores that are compatible with Prometheus
- Add FailingCommandsIT and CommandSubmissionCompletion to Ledger test tool suite. Some of the tests previously part of the CommandService Ledger API Test Tool suite have been moved to a new home in CommandSubmissionCompletion to reflect the fact that those use the submission/completion workflow instead of leveraging the submit-and-wait alternatives.

DAML Triggers - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Expose timestamp in triggers.
  See `#3612 <https://github.com/digital-asset/daml/issues/3612>`__.

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~

- Fix and document ``/contracts/lookup`` endpoint. See #3755.
- Expose exercise result. Changed the output
  of the ``/command/exercise``. Note ``exerciseResult`` and ``contracts``
  in ``{"status":200,"result":{"exerciseResult": ...,"contracts":[...]}``.
  See #3314.

Sandbox
~~~~~~~

- Restore 0.13.38 logging behaviour.

Navigator
~~~~~~~~~

- Restore 0.13.38 logging behaviour.

Extractor
~~~~~~~~~

- Restore 0.13.38 logging behaviour.

Internals
~~~~~~~~~

- As of 0.13.39, we merged a number of internal JAR files in
  the SDK tarball to reduce its size. These jars used to be standalone
  JARs you could invoke as e.g. ``java -jar sandbox.jar <args>``. As a
  result of merging the jars, they lost their individual ``logback.xml``
  configuration file. Although running the jars directly was (and is
  still) not supported, note that you can now achieve the same behaviour
  with e.g. ``java -Dlogback.configurationFile=sandbox-logback.xml -jar
  daml-sdk.jar sandbox <args>``.

DAML Standard Library
~~~~~~~~~~~~~~~~~~~~~

- Add ``Eq`` instances for ``AnyTemplate``, ``AnyChoice`` and ``AnyContractKey``.

DAML Compiler
~~~~~~~~~~~~~

- Fix an issue where transitive package dependencies
  resulted in packages not being found, if the DAR name was changed with
  `-o`.

Documentation
~~~~~~~~~~~~~

- Added documentation for authorization claims


.. _release-0-13-40:

0.13.40 - 2019-12-10
--------------------

DAML Compiler
~~~~~~~~~~~~~

- The modules DA.Types and GHC.Tuple from daml-prim have been moved to separate DALF packages.
- Fixed an issue where packages produced by damlc resulted in type errors during validation by DAML engine.


Sandbox
~~~~~~~

- The sandbox JWT authentication now respects the ledgerId and participantId fields of the token payload.
- Improve loading of active contracts for the Sandbox SQL backend.
- AuthService implementations can now restrict the validity of access tokens to a single ledger or participant.

Java Client
~~~~~~~~~~~

- Ensure the access token is initialized when using a deprecated constructor.

RxJava Bindings
~~~~~~~~~~~~~~~

- Added a method to the ``Bot`` class allowing users to specify a ``Scheduler`` to use for running the bot. See `issue #2356 <https://github.com/digital-asset/daml/issues/2356>`__.

Java Bindings
~~~~~~~~~~~~~

- Removed warnings in code emitted by the Java Codegen.


.. _release-0-13-39:

0.13.39 - 2019-12-05
--------------------

Java Bindings
~~~~~~~~~~~~~

- Added authentication support. See
  `issue #3626 <https://github.com/digital-asset/daml/issues/3626>`__.

DAML Compiler
~~~~~~~~~~~~~

- The modules ``GHC.Prim`` and ``GHC.Types`` from ``daml-prim``
  have been moved to separate packages.
- Don't make ``UndecidableSuperClasses`` a default language extension
  for DAML anymore. If you really need this feature for a module,
  you can reenable it using a ``LANGUAGE`` pragma at the top.

DAML SDK
~~~~~~~~

- Reduced the size of the DAML SDK by about 60% uncompressed, 70%
  compressed, by deduplicating Scala dependencies.
- ``daml damlc docs`` now takes into account the project's
  ``build-options`` from ``daml.yaml``.
- ``daml ledger navigator`` now loads ``frontend-config.js`` properly.

Navigator
~~~~~~~~~

- Explicit config files passed via ``-c`` are preferred
  over ``daml.yaml``.

Ledger API Server
~~~~~~~~~~~~~~~~~

- Add a health check endpoint conforming to the
  `GRPC Health Checking Protocol <https://github.com/grpc/grpc/blob/master/doc/health-checking.md>`_.
- Add health checks for index database connectivity.

Participant State API
~~~~~~~~~~~~~~~~~~~~~

- Add a mandatory ``currentHealth()`` method to ``IndexService``,
  ``ReadService`` and ``WriteService``.


DAML Triggers - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- DAML triggers can now be run against an authenticated ledger.

DAML Script - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~

- Add createAndExerciseCmd matching the Ledger API command of the same name.


.. _release-0-13-38:

0.13.38 - 2019-11-29
--------------------

Ledger API
~~~~~~~~~~

- Allow non-alphanumeric characters in Ledger API server participant ids
  (space, colon, hash, slash, dot). Proper fix for change originally
  attempted in v0.13.36. See issue
  `issue #3327 <https://github.com/digital-asset/daml/issues/3327>`__.
- Add healthcheck endpoints, conforming to the
  `GRPC Health Checking Protocol <https://github.com/grpc/grpc/blob/master/doc/health-checking.md>`_.
  It is always ``SERVING`` for now.

Ledger API Server
~~~~~~~~~~~~~~~~~

- Ledger API Server and Indexer now accept an instance of ``MetricRegistry``
  as parameters. This gives implementors of ledger integrations the most
  flexibility to set up metrics reporting that works best for them.
- Add various metrics to track gRPC requests, command submissions, and state
  update processing.
  See `#3513 <https://github.com/digital-asset/daml/issues/3513>`__.

DAML Ledger Integration Kit
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Add conformance test coverage for the ``grpc.health.v1.Health`` service.
- Add Ledger API Test Tool `--load-scale-factor` option that allows dialing up
  or down the workload applied by scale tests (such as the
  ``TransactionScaleIT`` suite). This allows improving the performance of
  different ledger over time.
- The Ledger API Test Tool no longer shows individual test duration colored
  based on how long they lasted.

Sandbox
~~~~~~~

- Add support for JWT tokens that only authorize to read data, but not to act
  on the ledger.
- Add CLI options to start the sandbox with JWT based authentication with RSA
  signed tokens.
  See `issue #3155 <https://github.com/digital-asset/daml/issues/3155>`__ .
- The ``--auth-jwt-hs256`` CLI option is renamed to
  ``--auth-jwt-hs256-unsafe``: you are advised to _not_ use this JWT token
  signing way in a production environment.

Navigator
~~~~~~~~~

- Fixed a bug where the ``--access-token-file`` option did not work correctly.

DAML Compiler
~~~~~~~~~~~~~

- Bugfix: The ``Sdk-Version`` field in a DAR manifest file now matches the SDK
  version of the compiler, not the ``sdk-version`` field from ``daml.yaml``.
  These are usually the same, but they could be different if you set the
  ``DAML_SDK_VERSION`` environment variable before running ``daml init`` or
  ``daml build``.
- Make the experimental feature "generic templates"
  unavailable. The current implementation is at odds with other, more
  important language features still under development.

DAML Studio
~~~~~~~~~~~

- Notify users about new DAML Driven blog posts.

Java Bindings
~~~~~~~~~~~~~

- Deprecated existing constructors for ``DamlLedgerClient``, please use
  the static ``newBuilder`` method to instantiate a builder and use it to
  create the client, starting from either a ``NettyChannelBuilder`` or a
  plain host/port pair.
- Rename ``DamlMap`` to ``DamlTextMap``.
- ``DamlCollectors`` class provides Collectors to build more easily
  ``DamlList`` and ``DamlTextMap``.
- Change the recommended method to convert ``DamlValue`` containers
  from/to Java Bindings containers.
  See `docs/source/app-dev/bindings-java/codegen.rst` for more details
  the new methodology.


DAML-LF Interface Reader
~~~~~~~~~~~~~~~~~~~~~~~~

- **Rename** ``PrimTypeMap`` to ``PrimTypeTextMap`` and ``PrimType.Map`` to
  ``PrimType.TextMap``

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~

- Accept a path to a file containing a token at startup for package retrieval.
  See `issue #3627 <https://github.com/digital-asset/daml/issues/3627>`__.

DAML Triggers - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- DAML Triggers now allow you to specify which templates you want to listen
  for. This can improve performance.

DAML Script - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- DAML Script can now run be used in distributed topologies.
- Expose the Ledger API ``exerciseByKey`` command


.. _release-0-13-37:

0.13.37 - 2019-11-20
--------------------

DAML Stdlib
~~~~~~~~~~~

- Added the ``NumericScale`` typeclass, which improves the type inference for Numeric literals, and helps catch the creation of out-of-bound Numerics earlier in the compilation process.
- ``fromAnyChoice`` and ``fromAnyContractKey`` now take
  the template type into account.

Navigator
~~~~~~~~~

- Fixed a bug where Navigator becomes unresponsive if the ledger does not contain any DAML packages.

Ledger-API
~~~~~~~~~~

- Add field ``gen_map`` in Protobuf definition for ledger
  api values. This field is used to support generic maps, an new
  feature currently in development.  See issue
  https://github.com/digital-asset/daml/issues/2256 for more details
  about generic maps.
  The Ledger API will send no messages where this field is set, when
  using a stable version of DAML-LF.  However the addition of this
  field may cause pattern-matching exhaustive warnings in the code of
  ledger API clients. Those warnings can be safely ignored until
  GenMap is made stable in an upcoming version of DAML-LF.

Extractor
~~~~~~~~~

- The app can now work against a Ledger API server that requires client authentication. See `issue #3157 <https://github.com/digital-asset/daml/issues/3157>`__.

DAML Compiler
~~~~~~~~~~~~~

- **Breaking** The default DAML-LF version is now 1.7. You can still
  produce DAML-LF 1.6 by passing ``--target=1.6`` to ``daml
  build``. This removes the ``Decimal`` type in favor of a ``Numeric
  s`` type with a flexible scale. ``Decimal`` is now a synonym for
  ``Numeric 10``. If you get errors about ambigous literals, you might
  need to add a type annotation, e.g., replace ``1.0`` by ``(1.0 : Decimal)``.

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~

- CLI configuration to enable serving static content as part of the JSON API daemon:
  ``--static-content "directory=/full/path,prefix=static"``
  This configuration is NOT recommended for production deployment. See issue #2782.
- The database schema has changed; if using
  ``--query-store-jdbc-config``, you must rebuild the database by adding
  ``,createSchema=true``.
  See `issue #3461 <https://github.com/digital-asset/daml/pull/3461>`_.
- Terminate process immediately after creating schema. See issue #3386.

DAML Triggers - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``emitCommands`` now accepts an additional argument
  that allows you to mark contracts as pending. Those contracts will
  be automatically filtered from the result of ``getContracts`` until
  we receive the corresponding completion/transaction.

DAML Script - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~

- This release contains a first version of an experimental DAML script
  feature that provides a scenario-like API that is run against an actual ledger.

.. _release-0-13-36:

0.13.36 - 2019-11-14
--------------------

Ledger
------

- Fix divulged contract visibility in multi-participant environments. See `issue #3351 <https://github.com/digital-asset/daml/issues/3351>`__.
- Enable the ability to configure ledger api servers with a time service (for test purposes only).
- Allow a ledger api server to share the DAML engine with the DAML-on-X participant node for performance. See `issue #2975 <https://github.com/digital-asset/daml/issues/2975>`__.
- Allow non-alphanumeric characters in ledger api server participant ids (space, colon, hash, slash, dot).
- Include SQL statement type in ledger api server logging of SQL errors.

DAML Compiler
-------------

- Support for incremental builds in ``daml build`` using the ``--incremental=yes`` flag.
  This is still experimental and disabled by default but will become enabled by default in the future.
  On large codebases, this can significantly improve compile times and reduce memory usage.
- Support for data dependencies on packages compiled with an older SDK
  (experimental). To import data dependencies, list the packages under the ``data-dependencies``
  stanza in the project's daml.yaml file.

Sandbox
-------

- Add the option to start the sandbox with JWT based authentication. See `issue #3363 <https://github.com/digital-asset/daml/issues/3363>`__.
- Fixed a bug in the SQL backend that caused the database to be flooded with requests when streaming out transactions.

DAML Stdlib
-----------

- ``maintainer`` function that will give you the list of maintainers of a contract key.

DAML Triggers
-------------

- Added ``exerciseByKeyCmd`` and ``dedupExerciseByKey`` to exercise a choice given the contract key instead of the contract id.
- ``getTemplates`` has been renamed to ``getContracts`` to describe its behavior more accurately.
  ``getTemplates`` still exists as a compatiblity helper but it is deprecated and will be removed in a future SDK release.
- Fix a bug where the use of Numeric caused triggers to crash with an assertion error.

JSON API - Experimental
-----------------------

- Fix to support Archive choice. See issue #3219
- Implement replay on database consistency violation, See issue #3387.
- Comparison/range queries supported.
  See `issue #2780 <https://github.com/digital-asset/daml/issues/2780>`__.

Extractor - Experimental
------------------------

- Fix bug in reading TLS parameters.


.. _release-0-13-34:

0.13.34 - 2019-11-07
--------------------

DAML-LF - Internal
~~~~~~~~~~~~~~~~~~

- Freeze DAML-LF 1.7. Summary of changes (See DAML-LF specification for more details.):
   + Add support for parametrically scaled Numeric type.
   + Drop support of Decimal in favor or Numerics.
   + Add interning of strings and names. This reduces drastically dar file size.
   + Add support for 'Any' type.
   + Add support for type representation values.

- Add immutable bintray/maven packages for handling DAML-LF archive up to version 1.7:
   + `com.digitalasset.daml-lf-1.7-archive-proto`

     This package contains the archive protobuf definitions as they
     were introduced when 1.7 was frozen.  These definitions can be
     used to read DAML-LF archives up to version 1.7.

DAML Triggers
~~~~~~~~~~~~~
- Triggers must now be compiled with ``daml build --target 1.7`` instead of ``1.dev``.


.. _release-0-13-33:

0.13.33 - 2019-11-06
--------------------

Navigator
~~~~~~~~~
- Fixed regression in Navigator to properly respect the CLI option ``--ledger-api-inbound-message-size-max`` again. See `issue #3301 <https://github.com/digital-asset/daml/issues/3301>`__.

DAML Compiler
~~~~~~~~~~~~~
- Reduce the memory footprint of the IDE and the command line tools (ca. 18% in our experiments).
- Fix compile error caused by instantiating generic templates at ``Numeric n``.
- The compiler now accepts single-constructor enum types. For example ``data A = A`` or ``data Foo = Bar``.

DAML Triggers
~~~~~~~~~~~~~
- Add ``dedupCreate`` and ``dedupExercise`` helpers that will only send
  commands if they are not already in flight.
- Remove the custom ``AbsoluteContractId`` type in favor of the regular ``ContractId`` type used in DAML templates.

Sandbox
~~~~~~~
- Fixed a bug a database migration script for Sandbox on Postgres introduced in SDK 0.13.32. See `issue #3284 <https://github.com/digital-asset/daml/issues/3284>`__.
- Timing about database operations are now exposed over JMX as well as via the logs.
- Added a missing index to the SQL schema for the Postgres Ledger.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~
- Re-add :doc:`integration kit documentation </daml-integration-kit/index>` that got accidentally deleted.

Ledger API
~~~~~~~~~~
- Disallow empty commands. See `issue #592 <https://github.com/digital-asset/daml/issues/592>`__.

DAML Stdlib
~~~~~~~~~~~
- Add `DA.TextMap.filter` and `DA.Next.Map.filter`.
- Add `assertEq` and `assertNotEq` to `DA.Assert` as synonyms for `===` and `=/=`.
- Add ``DA.Foldable.mapA_``, ``DA.Foldable.forA_``, ``DA.Foldable.sequence_`` and ``DA.Action.replicateA_``. These functions
  match the behavior of corresponding functions without the underscore suffix but ignore the result which can be more convenient and
  efficient.

Extractor - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~
- Extractor now stores exercise events in the single table data format. See `issue #3274 <https://github.com/digital-asset/daml/issues/3274>`__.

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~
- ``workflowId`` no longer included in any responses.
- ``/contracts/search`` endpoint can optionally store searched
  contracts in a Postgres-based cache, by passing the new ``--query-store-jdbc-config`` option.
  See `issue #2781 <https://github.com/digital-asset/daml/issues/2781>`_.

DAML SDK
~~~~~~~~
- Display release notes in the IDE when the DAML extension is upgraded.


.. _release-0-13-32:

0.13.32 - 2019-10-29
--------------------

DAML Triggers
~~~~~~~~~~~~~

- The trigger runner now supports triggers using the high-level API directly. These no longer need to be converted to low-level Triggers using ``runTrigger``. Triggers using the low-level API are still supported.
- The trigger runner has a new command that just lists the triggers in
  a dar using ``daml trigger list --dar path/to/dar``.

DAML Compiler
~~~~~~~~~~~~~

- The package database is now be cleaned automatically on initialization.
  This means that you should no longer have to run ``daml clean`` on SDK upgrades
  if you use DAR dependencies (e.g. with DAML triggers).

Sandbox
~~~~~~~

- Improve performance of looking up contracts from postgres. See `issue #2330 <https://github.com/digital-asset/daml/issues/2330>`__.


.. _release-0-13-31:

0.13.31 - 2019-10-18
--------------------

Sandbox
~~~~~~~

- Party management fix, see `issue #3177 <https://github.com/digital-asset/daml/issues/3177>`_.
- The maximum allowed TTL for commands is now configurable via the ``--max-ttl-seconds`` parameter, for example: ``daml sandbox --max-ttl-seconds 300``.
- Fixed a bug where ``CreatedEvent#event_id`` field is not properly filled by ``ActiveContractsService``.
  See `issue #65 <https://github.com/digital-asset/daml/issues/65>`__.

DAML SDK
~~~~~~~~

- Shrink docker image containing the full DAML SDK from 2.8 GB to 1.2 GB.

Navigator
~~~~~~~~~

- Accept and use an access token to be used against Ledger API servers that require authentication, see `issue #3156 <https://github.com/digital-asset/daml/issues/3156>`_.
- Demo-oriented password workflow has been removed.

Ledger Client
~~~~~~~~~~~~~

- Expose new method to construct channels for more granular control over the client creation process.

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~

-  Add ``/parties`` endpoint.

DAML Triggers - Experimental
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- The trigger runner now logs output from ``trace``, ``error`` and
  failed command completions and hides internal debugging output.

DAML-LF - Internal
~~~~~~~~~~~~~~~~~~

- Changed the name of the bintray/maven package from ``com.digitalasset.daml-lf-archive-scala`` to ``com.digitalasset.daml-lf-archive-reader``

.. _release-0-13-30:

0.13.30 - 2019-10-15
--------------------

DAML Standard Library
~~~~~~~~~~~~~~~~~~~~~

- Add ``DA.Action.State`` module containing a ``State`` action that
  can be used for computations that modify a state variable.

- Add ``createAndExercise``.

DAML Compiler
~~~~~~~~~~~~~

- Fixed the location of interface files when the
  ``source`` field in ``daml.yaml`` points to a file. This is mainly
  important for when you want to use the created ``.dar`` in the
  ``dependencies`` field of another package.
  See `issue #3135 <https://github.com/digital-asset/daml/issues/3135>`_.

DAML-LF
~~~~~~~

- **Breaking** Rename DAML-LF Archive protobuf package from
  `com.digitalasset.daml_lf` to `com.digitalasset.daml_lf_dev`. This
  will only affect you do not use the DAML-LF Archive reader provided
  with the SDK but a custom one based on code generation by protoc.

- **Breaking** Some bintray/maven packages are renamed:
   + `com.digitalasset.daml-lf-proto` becomes
     `com.digitalasset.daml-lf-dev-archive-proto`
   + `com.digitalasset.daml-lf-archive` becomes
     `com.digitalasset:daml-lf-dev-archive-java-proto``

- Add immutable bintray/maven packages for handling DAML-LF archive up to version 1.6 in a stable way:
   + `com.digitalasset.daml-lf-1.6-archive-proto`

     This package contains the archive protobuf definitions as they
     were introduced when 1.6 was frozen.  These definitions can be
     used to read DAML-LF archives up to version 1.6.

     The main advantage of this package over the `dev` version
     (`com.digitalasset.daml-lf-dev-archive-proto`) is that it is
     immutable (it is guaranteed to never changed once introduced
     in the SDK). In other words one can used it without suffering
     frequent breaking changes introduced in the `dev` version.

     Going forward the SKD will contain a similar immutable package
     containning the proto definition for at least each DAML-LF
     version the compiler supports.

     We strongly advise anyone reading DAML-LF Archive directly to use
     this package (or the
     `com.digitalasset:daml-lf-1.6-archive-java-proto` package
     described below).  Breaking changes to the `dev` version may be
     introduced frequently and without further notice in the release
     notes.

   + `com.digitalasset:daml-lf-1.6-archive-java-proto`

     This package contains the java classes generated from the package
     `com.digitalasset.daml-lf-1.6-archive-proto`


DAML Triggers
~~~~~~~~~~~~~

- This release contains a first version of an experimental DAML
  triggers feature that allows you to implement off-ledger automation
  in DAML.


DAML-SDK Docker Image
~~~~~~~~~~~~~~~~~~~~~

- The image now contains a ``daml`` user and the SDK is installed to ``/home/daml/.daml``.
  ``/home/daml/.daml/bin`` is automatically added to ``PATH``.

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~

- Support for automatic package reload
  See `issue #2906 <https://github.com/digital-asset/daml/issues/2906>`_.


Java Bindings
~~~~~~~~~~~~~

- Add helper to prepare transformer for ``Bot.wire``. See `issue #3097 <https://github.com/digital-asset/daml/issues/3097>`_.

Ledger
~~~~~~

- The ledger api index server starts only after the indexer has finished initializing the database.

Sandbox
~~~~~~~

- Filter contracts or contracts keys in the database query for parties that cannot see them.

Scala Bindings
~~~~~~~~~~~~~~

- Fixed a bug in the retry logic of ``LedgerClientBinding#retryingConfirmedCommands``. Commands are now only retried when the server responds with status ``RESOURCE_EXHAUSTED`` or ``UNAVAILABLE``.

Scala Codegen
~~~~~~~~~~~~~

- Fixes for StackOverflowErrors in reading large LF archives. See `issue #3104 <https://github.com/digital-asset/daml/issues/3104>`_.

SQL Extractor
~~~~~~~~~~~~~

- The format used for storing Optional and Map values found in contracts
  as JSON has been replaced with :doc:`/json-api/lf-value-specification`.  See `issue
  #3066 <https://github.com/digital-asset/daml/issues/3066>`_ for specifics.

.. _release-0-13-29:

0.13.29 - 2019-10-04
--------------------

- Rerelease of 0.13.28 since that failed due to CI issues.

.. _release-0-13-28:

0.13.28 - 2019-10-04
--------------------

JSON API - Experimental
~~~~~~~~~~~~~~~~~~~~~~~

- Returning archived and active/created contracts from ``/command/exercise``
  endpoint. See `issue #2925 <https://github.com/digital-asset/daml/issues/2925>`_.
- Flattening the output of the ``/contracts/search`` endpoint.
  The endpoint returns ``ActiveContract`` objects without ``GetActiveContractsResponse`` wrappers.
  See `issue #2987 <https://github.com/digital-asset/daml/pull/2987>`_.

SDK
~~~

- Bundle the ``daml-trigger`` package. Note, this package is experimental and will change.
- Releases can now bundle additional libraries with the SDK in ``$DAML_SDK/daml-libs``. You
  can refer to them in your ``daml.yaml`` file by listing the package name without ``.dar``
  extension. See `issue #2979 <https://github.com/digital-asset/daml/issues/2979>`_.

DAML Studio
~~~~~~~~~~~

- ``damlc ide`` now also supports a ``--target`` option.
  The easiest way to specify this is the ``build-options`` field in ``daml.yaml``.
- Fix a bug where the same module was imported twice
  under different file paths caused module name
  collisions. See `issue #3099 <https://github.com/digital-asset/daml/issues/3099>`_.

Ledger
~~~~~~

- Improve SQL backend performance by eliminating extra queries to the database.
- Enhance logging to correlate log messages with the associated participant id in multi-participant node tests and environments
- Ledger api server indexer closes akka system on shutdown.
- The ledger api server now stores divulged, otherwise unknown contracts.

DAML Visualization
~~~~~~~~~~~~~~~~~~

- Adding `daml damlc visual-web` command. visual-command generates webpage with `d3 <https://d3js.org>`_ network.

DAML Ledger Integration Kit
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- The transaction service is now fully tested.
- The TTL for commands is now read from the configuration service.
- The contract key tests now live under a single test suite and are multi-node aware.

DAML Compiler
~~~~~~~~~~~~~

- Fix a problem where constraints of the form ``Template (Foo t)`` caused the compiler to suggest enabling the ``UndecidableInstances`` language extension.
- Generic template instantiations like ``template instance IouProposal = Proposal Iou`` now generate a type synonym ``type IouProposal = Proposal Iou`` that can be used in DAML. Before, they generated a ``newtype``, which cannot be used anymore.
- Fixed a bug where ``damlc build`` sometimes did not find modules during typechecking
  even if they were present during parallel compilations.

Security
~~~~~~~~

- Document how to verify the signature on release tarballs.

.. _release-0-13-27:

0.13.27 - 2019-09-25
--------------------

DAML Assistant
~~~~~~~~~~~~~~

- ``daml start`` now supports ``--sandbox-option=opt``, ``--navigator-option=opt``
  and ``--json-api-option=opt`` to pass additional option to sandbox/navigator/json-api.
  These flags can be specified multiple times.

DAML Compiler
~~~~~~~~~~~~~

- Fix a bug where generic templates could crash the compiler.

Security
~~~~~~~~

- Fix signing process.

.. _release-0-13-26:

0.13.26 - 2019-09-24
--------------------

JSON API
~~~~~~~~

- ``/contracts/search`` now supports a query language for filtering the
  contracts returned by matching fields.  See `issue 2778
  <https://github.com/digital-asset/daml/issues/2778>`_.

DAML Compiler
~~~~~~~~~~~~~

- Fix a bug where ``.dar`` files produced by ``daml build`` were missing
  all ``.daml`` files except for the one that ``source`` pointed to.
- Fix a bug where importing the same module from different directories
  resulted in an error in ``daml build``.
- ``damlc migrate`` now produces a project that can be built with ``daml build`` as opposed to
  having to use the special ``build.sh`` and ``build.cmd`` scripts.

DAML Integration Toolkit
~~~~~~~~~~~~~~~~~~~~~~~~

- 30 more test cases have been added to the transaction service test suite.

Security
~~~~~~~~

- Starting with this one, releases are now signed on GitHub.

.. _release-0-13-25:

0.13.25 - 2019-09-18
--------------------

Documentation
~~~~~~~~~~~~~

- Suppress instance documentation when `--data-only` mode is requested.

DAML-LF
~~~~~~~

- Add CAST_NUMERIC and SHIFT_NUMERIC in DAML-LF 1.dev.
- Change signature of MUL_NUMERIC and DIV_NUMERIC.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- Fix contract key uniqueness check in kvutils.

- Preload packages in a background thread in kvutils.

Ledger
~~~~~~

- ActiveContractsService now specifies to always return at least one message with the offset. This removes a special case where clients would need to check if the stream was empty or not.

- Dramatically increased performance of the ActiveContractService by only loading the contracts that the parties in the transaction filter are allowed to see.

.. _release-0-13-24:

0.13.24 - 2019-09-16
--------------------

Java codegen
~~~~~~~~~~~~

- If the DAR source cannot be read, the application crashes and prints an error report.

DAML Assistant
~~~~~~~~~~~~~~

- Java and Scala codegen is now integrated with the
  assistant and distributed with the SDK. It can be run via ``daml codegen``.
  You can find more information in the `DAML Assistant documentation <https://docs.daml.com/tools/assistant.html>`_.

DAML Compiler
~~~~~~~~~~~~~

- Fix bug with qualified imports of generic templates.

Ledger
~~~~~~

- Upgraded ledger-api server H2 Database version to 1.4.199 with stability fixes including one to the ``merge`` statement.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- One more test case added. Transaction service tests are not multi-node aware.
- Semantic tests now ensure synchronization across participants when running in a multi-node setup.

.. _release-0-13-23:

0.13.23 - 2019-09-11
--------------------

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- The reference implementation can now spin up multiple nodes, either scaling
  a single participant horizontally or adding new participants. Check the CLI ``--help`` option.
- The test tool now runs the double spend test on a shared contract in a
  multi-node setup (as well as single-node).
- The test tool can now run all semantic test in a multi-node setup.

DAML Standard Library
~~~~~~~~~~~~~~~~~~~~~

- **BREAKING CHANGE** The ``(/)`` operator was moved out of the ``Fractional`` typeclass into a separate ``Divisible`` typeclass, which is now the parent class of ``Fractional``. The ``Int`` instance of ``Fractional`` is discontinued, but there is an ``Int`` instance of ``Divisible``. This change will break projects that rely on the ``Fractional Int`` instance. To fix that, change the code to rely on ``Divisible Int`` instead. This change will also break projects where a ``Fractional`` instance is defined. To fix that, add a ``Divisible`` instance and move the definition of ``(/)`` there.

DAML Assistant
~~~~~~~~~~~~~~

- The HTTP JSON API is now integrated with the
  assistant and distributed with the SDK. It can either be launched
  via ``daml json-api`` or via ``daml start``. You can find more information in the
  `README <https://github.com/digital-asset/daml/blob/master/ledger-service/http-json/README.md>`_.
- The `daml.yaml` file now supports an additional field
  `build-options`, which you can use to list cli options you want added to
  invocations of `daml build` and `daml ide`.

JSON API
~~~~~~~~

- **BREAKING CHANGE** The ``/contracts/search`` request payload must use
  ``"%templates"`` in place of ``"templateIds"`` to select which templates' contracts are
  returned.  See `issue #2777 <https://github.com/digital-asset/daml/issues/2777>`_.

DAML Compiler
~~~~~~~~~~~~~

- **BREAKING CHANGE** Move the DAML-LF produced by generic template instantiations closer to the surface syntax. See the documentation on `How DAML types are translated to DAML-LF <https://docs.daml.com/app-dev/daml-lf-translation.html#template-types>`__ for details.

.. _release-0-13-22:

0.13.22 - 2019-09-04
--------------------

DAML Assistant
~~~~~~~~~~~~~~

- **BREAKING CHANGE** Changed the meaning of the ``source`` field in the daml.yaml
  file to be a pointer to the source directory of the DAML code contained in a project relative to
  the project root. This is breaking projects, where the ``source`` field of the project is pointing
  to a non-toplevel location in the source code directory structure.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- Introduced initial support for multi-node testing. Note that for the time
  being no test actually uses more than one node.
- **BREAKING CHANGE** The ``-p`` / ``--target-port`` and ``-h`` / ``--host``
  flags have been discontinued. Pass one (or more) endpoints to test as command line arguments in the
  ``<host>:<port>`` form.

Documentation
~~~~~~~~~~~~~

- Basic explanation of generic templates.

Ledger API
~~~~~~~~~~

- **BREAKING CHANGE** In Protobuf ``Value`` message, rename ``decimal` field to ``numeric``.

Sandbox
~~~~~~~

- Updated the PostgreSQL JDBC driver to version 42.2.6.
- Added TRACE level debugging for database operations.
- Fixed a bug that could lead to an inconsistent snapshot of active contracts being served
  by the ActiveContractsService under high load.
- Commands are now deduplicated based on ``(submitter, application_id, command_id)``.

.. _release-0-13-21:

0.13.21 - 2019-08-29
--------------------

DAML Compiler
~~~~~~~~~~~~~

- Enable the language extension ``FlexibleContexts`` by default.
- **BREAKING CHANGE** Enable the language extension ``MonoLocalBinds`` by default. ``let`` and ``where`` bindings introducing polymorphic functions that are used at different types now need an explicit type annotation. Without the type annotation the type of the first use site will be inferred and use sites at different types will fail with a type mismatch error.

Java Codegen
~~~~~~~~~~~~

- Fix bug that caused the generation of duplicate methods that affected sources with data constructors with type parameters that are either non-unique or not presented in the same order as in the corresponding data type declaration. See `#2367 <https://github.com/digital-asset/daml/issues/2367>`__.

Ledger
~~~~~~

- H2 Database support in the Ledger API Server.

Sandbox
~~~~~~~

- The sandbox now properly sets the connection pool properties ``minimumIdle``, ``maximumPoolSize``, and ``connectionTimeout``.

.. _release-0-13-20:

0.13.20 - 2019-08-22
--------------------

Documentation
~~~~~~~~~~~~~

- Added platform-independent tips for testing

DAML Compiler
~~~~~~~~~~~~~

- Some issues that caused ``damlc test`` to crash on shutdown have been fixed.
- The DAML compiler was accidentally compiled without
  optimizations on Windows. This has been fixed which should improve
  the performance of ``damlc`` and ``daml studio`` on Windows.
- ``damlc build`` should no longer leak file handles so
  ``ulimit`` workarounds should no longer be necessary.
- Allow more contexts in generic templates. Specifically, template constraints can
  have arguments besides type variables, if the FlexibleContexts extension is enabled.

DAML-LF
~~~~~~~

- **Breaking** Rename ``NUMERIC`` back to ``DECIMAL`` in Protobuf definition.

DAML Studio
~~~~~~~~~~~

- ``damlc ide`` now also accepts ``--ghc-option`` arguments like ``damlc build``
  so ``damlc ide --ghc-option -W`` launches the IDE with more warnings.
- The VSCode extension now has a configuration field for
  passing extra arguments to ``damlc ide``.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- Participant State API and kvutils was extended with support for
  changing the ledger configuration. See changelog in respective ``package.scala`` files.

Sandbox
~~~~~~~

- Fixed a bug that caused the reset service to hang for 10 seconds. See issue `#2549 <https://github.com/digital-asset/daml/issues/2549>`__.

Java Bindings
~~~~~~~~~~~~~

- The Java Codegen now supports parametrized ContractIds.
  See `#2258 <https://github.com/digital-asset/daml/issues/2258>`__.

DAML Standard Library
~~~~~~~~~~~~~~~~~~~~~

- Add ``stripInfix`` function to ``DA.List``.

.. _release-0-13-19:

0.13.19 - 2019-08-14
--------------------

Sandbox
~~~~~~~

- Fixed a bug that prevented the ledger from loading transactions with empty workflow ids.
- Fixed internal shutdown order to avoid dead letter warnings when
  stopping Sandbox/Ledger API Server.  See issue `#1886
  <https://github.com/digital-asset/daml/issues/1886>`__.

DAML Studio
~~~~~~~~~~~

- Added a new command for visualizing a project in the IDE.
- Print stack trace when a scenario fails.
- Various memory leaks have been fixed so long-running sessions should
  no longer show a significant increase in memory usage.

DAML Compiler
~~~~~~~~~~~~~

- The ``--project-root`` option now works properly with relative paths
  in ``daml build``.
- Support generic template declarations and instances. Documentation
  for generic templates is still being worked on.
- The ``--dump-pom`` flag from ``damlc package`` has been removed as
  packaging has not relied on POM files for a while.

Navigator
~~~~~~~~~

- ``{"None": {}}`` and ``{"Some": value}``, where previously accepted, are no longer supported or used for DAML ``Optional`` values.
  Instead, for simple cases, use the plain value for ``Some``, and ``null`` for ``None``.
  See issue `#2361 <https://github.com/digital-asset/daml/issues/2361>`__ for other cases.

HTTP JSON API
~~~~~~~~~~~~~

- A new, more intuitive JSON format for DAML values is supported.
  See issue `#2361 <https://github.com/digital-asset/daml/issues/2361>`__.


.. _release-0-13-18:

0.13.18 - 2019-08-07
--------------------

- Fix a bug where ``daml studio`` did not launch VSCode on Windows.

.. _release-0-13-17:

0.13.17 - 2019-08-07
--------------------

DAML Docs
~~~~~~~~~

- For ``damlc docs``, the ``--template`` argument now takes the path
  to a Mustache template when generating Markdown, Rst, and HTML
  output. The template can use ``title`` and ``body`` variables to
  control the appearance of the docs.

DAML Assistant
~~~~~~~~~~~~~~

- Spaces in user names or other parts of file names should now be handled correctly.
- The ``daml deploy`` and ``daml ledger`` experimental commands were
  added. Use ``daml deploy --help`` and ``daml ledger --help`` to find
  out more about them.

.. _release-0-13-16:

0.13.16 - 2019-08-01
--------------------

DAML Compiler
~~~~~~~~~~~~~

- **BREAKING CHANGE** Handwritten instances of ``Template`` and ``Choice``
  typeclasses are no longer supported. All template constructs must be defined
  using declarations inside ``template`` syntax.

DAML Docs
~~~~~~~~~

- The ``damlc docs`` command now produces docs to a folder by default. Use the
  new ``--combine`` flag to output a single file instead.
- The ``damlc docs`` flag ``--prefix`` has been replaced with a ``--template``
  flag which allows for a more flexible template.
- The ``damlc docs`` flag ``--json`` has been dropped in favor of
  ``--format=json``.

Extractor
~~~~~~~~~

- **BREAKING CHANGE** Changed schema to accomodate removed field
  ``ExercisedEvent#contract_creating_event_id``. Existing database schemas are
  not compatible anymore with the newer version. The extractor needs to be run
  on an empty schema from Ledger Begin.

Java Bindings
~~~~~~~~~~~~~

- Add all packages of java bindings to the javadocs. See `#2280
  <https://github.com/digital-asset/daml/issues/2280>`__.
- **BREAKING CHANGE** Removed field
  ``ExercisedEvent#contract_creating_event_id``.  See `#2068
  <https://github.com/digital-asset/daml/issues/2068>`__.

Ledger API
~~~~~~~~~~

- **BREAKING CHANGE** Removed field
  ``ExercisedEvent#contract_creating_event_id``.  See `#2068
  <https://github.com/digital-asset/daml/issues/2068>`__.

Sandbox
~~~~~~~

- The active contract service correctly serves stakeholders. See `#2070
  <https://github.com/digital-asset/daml/issues/2070>`__.
- Added the ``--maxInboundMessageSize`` CLI parameter to set the maximux size
  of messages received through the Ledger API. If the value is not set the
  current default is preserved (4 MB).
- Makes package uploads idempotent and tolerate partial duplicates. See `#2130
  <https://github.com/digital-asset/daml/issues/2130>`__.

.. _release-0-13-15:

0.13.15 - 2019-07-25
--------------------

DAML Studio
~~~~~~~~~~~~

- Scenario links no longer disappear if the
  current file does not compile. The location is adjusted but this is done
  one a best effort basis and can fail if the scenario itself is modified.

DAML Compiler
~~~~~~~~~~~~~~

- Support reading of DAML-LF 1.5 again.

DAML-LF
~~~~~~~

- **Breaking** Rename ``DECIMAL`` by ``NUMERIC`` in archive Protobuf definition.

Ledger API
~~~~~~~~~~~

- **BREAKING**: Drop support for legacy identifier. The
  previously deprecated field ``name`` in ``Identifier`` message is not
  supported anymore. Use ``module_name`` and ``entity_name`` instead.

Navigator
~~~~~~~~~

- Fixed an issue when Navigator console did not see any contracts.
  See `#2271 <https://github.com/digital-asset/daml/issues/2271>`__.

Documentation
~~~~~~~~~~~~~~

- Improved the Maven pom.xml file for ``quickstart-java`` to better integrate with VS Code.
  See `#887 <https://github.com/digital-asset/daml/issues/887>`__.

Releases
~~~~~~~~

- Releases should now be announced on `the releases blog <https://blog.daml.com/release-notes>`__.

.. _release-0-13-14:

0.13.14 - 2019-07-22
--------------------

DAML Compiler
~~~~~~~~~~~~~

- Support reading of DAML-LF 1.5 again.

DAML Studio
~~~~~~~~~~~

VSCode scenario view improvements. Add a note in the IDE if:

- there is an open scenario view for a scenario that does no longer exist,
- there is an open scenario view for a scenario in a file that does no longer compile.

.. _release-0-13-13:

0.13.13 - 2019-07-16
--------------------

DAML Assistant
~~~~~~~~~~~~~~

- Fix VSCode path for use if not already in PATH on mac
- **BREAKING**: remove `--replace=newer` option.

DAML Studio
~~~~~~~~~~~

- Fix a bug where the extension seemed to disappear every other
  time VS Code was opened.
- DAML Studio now displays a “Processing” indicator on the bottom
  left while the IDE is doing work in the background.

Sandbox
~~~~~~~

- Fixing an issue around handling passTime in scenario loader
  See `#1953 <https://github.com/digital-asset/daml/issues/1953>`__.
- Remembering already loaded packages after reset
  See `#1979 <https://github.com/digital-asset/daml/issues/1979>`__.

DAML-LF
~~~~~~~

- Release version 1.6. This versions provides:

  + ``enum`` types. See `issue #105
    <https://github.com/digital-asset/daml/issues/105>`__ and `DAML-LF 1
    specification <https://github.com/digital-asset/daml/blob/master/daml-lf/spec/daml-lf-1.rst>`__
    for more details.

  + new builtins for (un)packing strings. See `issue #16
    <https://github.com/digital-asset/daml/issues/16>`__.

  + intern package IDs. See `issue #1614
    <https://github.com/digital-asset/daml/pull/1614>`__.


DAML Compiler
~~~~~~~~~~~~~

- Add support for DAML-LF ``1.6``. In particular:

  + **BREAKING CHANGE** Add support for ``enum`` types. DAML variant types
    that look like enumerations (i.e., those variants without type parameters
    and without arguments) are compiled to the new DAML-LF ``enum`` type when
    DAML-LF 1.6 target is selected. For instance the daml type declaration of
    the form::

      data Color = Red | Green | Blue

    will produce a DAML-LF ``enum`` type instead of DAML-LF ``variant`` type.
    This change is breaking, since this release makes DAML-LF ``1.6`` the
    default compiler output.

  + Add ``DA.Text.toCodePoints`` and ``DA.Text.fromCodePoints`` primitives to
    (un)pack strings.

  + Add support for DAML-LF intern package IDs.

- **BREAKING CHANGE** Make DAML-LF 1.6 the default output.
  This change activates the support of ``enum`` type describes above.

- **BREAKING CHANGE** Drop support for DAML-LF 1.5. Compiling to DAML-LF 1.6
  requires some changes regarding enum types to applications using the Ledger
  API, see above. (The ledger server still supports DAML-LF 1.5.)

Ledger API
~~~~~~~~~~

- Add support for ``enum`` types. Simple DAML ``variant`` types
  will be mapped to DAML-LF ``enum`` types when using a DAML-LF ``1.6``
  archive. Ledger API Value Protobuf provides the new ``Enum`` message.
  This message must be used to communicate this new data type throught the
  API.

Java Codegen
~~~~~~~~~~~~

- Add support for ``enum`` types. ``enum`` types are mapped to
  standard java enum. See `Generate Java code from DAML
  <https://github.com/digital-asset/daml/blob/master/docs/source/app-dev/bindings-java/codegen.rst>`__
  for more details.

Scala Codegen
~~~~~~~~~~~~~

- Add support for ``enum`` types.

Navigator
~~~~~~~~~

- Add support for ``enum`` types.

Extractor
~~~~~~~~~

- Add support for ``enum`` types.

DAML Docs
~~~~~~~~~

- Added links to type signatures in generated docs. Check out the updated
  `standard library docs <https://docs.daml.com/daml/reference/base.html>`__.

.. _release-0-13-12:

0.13.12 - 2019-07-09
--------------------

DAML Assistant
~~~~~~~~~~~~~~
- Fix VSCode path for use if not already in PATH on mac.
- Kill child processes on ``SIGTERM``. This means that killing
  ``daml sandbox`` will also kill the sandbox process.

DAML-LF
~~~~~~~
- Fixed regression that produced an invalid daml-lf-archive artefact.
  See `#2058 <https://github.com/digital-asset/daml/issues/2058>`__.

DAML Docs
~~~~~~~~~
- **BREAKING CHANGE** ``damlc docs`` now typechecks the source files before doc generation, to be able to use type information during doc generation. This may break existing doc builds.
- Added ``--package-name`` and ``--input-format`` flags to ``damlc docs``.


.. _release-0-13-11:

0.13.11 - 2019-07-08
--------------------

Sandbox
~~~~~~~
- The completion stream method of the command completion service uses the ledger end as a default value for the offset. See `#1913 <https://github.com/digital-asset/daml/issues/1913>`__.
- Fixed an issue when CompletionService returns offsets having inclusive semantics when used for re-subscription.
  See `#1932 <https://github.com/digital-asset/daml/pull/1932>`__.
- DAML-LF packages used by the sandbox are now stored in Postgres,
  allowing users to resume a Postgres sandbox ledger without having to again
  specify all packages through the CLI.
  See `#1929 <https://github.com/digital-asset/daml/issues/1929>`__.

Java Bindings
~~~~~~~~~~~~~
- Added overloads to the Java bindings ``CompletionStreamRequest`` constructor and the ``CommandCompletionClient`` to accept a request without an explicit ledger offset. See `#1913 <https://github.com/digital-asset/daml/issues/1913>`__.
- **DEPRECATION**: the ``CompletionStreamRequest#getOffset`` method is deprecated in favor of the non-nullable ``CompletionStreamRequest#getLedgerOffset``. See `#1913 <https://github.com/digital-asset/daml/issues/1913>`__.

Scala Bindings
~~~~~~~~~~~~~~
- Contract keys are exposed on CreatedEvent. See `#1681 <https://github.com/digital-asset/daml/issues/1681>`__.

Navigator
~~~~~~~~~
- Contract keys are show in the contract details page. See `#1681 <https://github.com/digital-asset/daml/issues/1681>`__.

DAML Standard Library
~~~~~~~~~~~~~~~~~~~~~
- **BREAKING CHANGE**: Remove the deprecated modules ``DA.Map``, ``DA.Set``, ``DA.Experimental.Map`` and ``DA.Experimental.Set``. Please use ``DA.Next.Map`` and ``DA.Next.Set`` instead.
- Add ``Sum`` and ``Product`` newtypes that
  provide ``Monoid`` instances based on the ``Additive`` and ``Multiplicative``
  instances of the underlying type.
- Add ``Min`` and ``Max`` newtypes that
  provide ``Semigroup`` instances based ``min`` and ``max``.

DAML Compiler
~~~~~~~~~~~~~
- The default output path for all artifacts is now in the ``.daml`` directory.
  In particular, the default output path for .dar files in ``daml build`` is now
  ``.daml/dist/<projectname>.dar``.

DAML Studio
~~~~~~~~~~~
- DAML Studio is now published as an extension in the Visual Studio Code
  marketplace. The ``daml studio`` command will now install the published extension by
  default, but will revert to the extension bundled with the DAML SDK if installation
  fails. You can get the old default behavior of always using the bundled extension
  by running ``daml studio --replace=newer`` or ``daml studio --replace=always`` instead.
- You can now configure the gRPC message size limit in
  ``daml.yaml`` via ``scenario-service: {"grpc-max-message-size": 1000000}``.
  This will set the limit to 1000000 bytes. This should
  only be necessary for very large projects.
- You can now configure the gRPC timeout
  ``daml.yaml`` via ``scenario-service: {"grpc-timeout": 42}``.
  This option will set the timeout to 42 seconds. You should
  only need to set this option for very large projects.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~
- Make DivulgenceIT properly work when run via the Ledger API Test Tool.
- The submission service shuts down its ExecutorService upon exit to ensure a smooth shutdown.

DAML-LF
~~~~~~~
- The DAML-LF development version (``1.dev``) includes a new, breaking restriction
  regarding contract key lookups. In short, when looking up or fetching a key,
  the transaction submitter must be one of the key maintainers.
  Note that this change is not breaking since the compiler does not produce DAML-LF
  ``1.dev`` by default. However it will be a breaking change once this restriction
  makes it into DAML-LF ``1.6`` and once DAML-LF ``1.6`` becomes the default.


.. _release-0-13-10:

0.13.10 - 2019-06-28
--------------------

Sandbox
~~~~~~~

- Added `--log-level` command line flag.
- **BREAKING CHANGE**: The Sandbox no longer supports loading from DALF files. You can now only use DAR files. See `#1610 <https://github.com/digital-asset/daml/issues/1610>`__.


Ledger API
~~~~~~~~~~

- Added new CLI flags ``--stable-party-identifiers`` and
  ``--stable-command-identifiers`` to the :doc:`Ledger API Test Tool
  </tools/ledger-api-test-tool/index>` to allow disabling randomization of party
  and command identifiers. It is useful for testing of ledgers which are
  configured with a predefined static set of parties.


.. _release-0-13-9:

0.13.9 - 2019-06-28
-------------------

DAML Studio
~~~~~~~~~~~

- Fix an error in the ``package.json`` that stopped the extension from being loaded.

.. _release-0-13-8:

0.13.8 - 2019-06-27
-------------------

Navigator
~~~~~~~~~

- Contract details now show signatories and observers.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.

Scala Bindings
~~~~~~~~~~~~~~

- Reflect addition of signatories and observers to the bindings.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.

Java Codegen
~~~~~~~~~~~~

- Generated code supports signatories and observers as exposed by the bindings.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.

Java Bindings
~~~~~~~~~~~~~

- Reflect addition of signatories and observers to the bindings.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.

Ledger API
~~~~~~~~~~

- Expose signatories and observers for a contract in ``CreatedEvent``.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.

- **BREAKING CHANGE**: Specify pretty C# namespaces in ledger api protos. C# bindings will end up in a different namespace than the default one.
  See `#1901 <https://github.com/digital-asset/daml/issues/1901>`__.

DAML Compiler
~~~~~~~~~~~~~

- **BREAKING CHANGE**: Drop support for DAML-LF 1.4. Compiling to DAML-LF 1.5 should work without any code changes, although we highly recommend not specifying a target DAML-LF version at all. (The ledger server still supports DAML-LF 1.4.)

Sandbox
~~~~~~~

- Made the archive CLI arguments optional.
  See `#1905 <https://github.com/digital-asset/daml/issues/1905>`__.

DAML-LF
~~~~~~~

- **BREAKING CHANGE**: Specify pretty C# namespaces in archive protos. C# bindings will end up in a different namespace than the default one.
  See `#1900 <https://github.com/digital-asset/daml/issues/1900>`__.

.. _release-0-13-7:

0.13.7 - 2019-06-26
-------------------

DAML-LF
~~~~~~~

- Rename ``none`` and ``some`` to ``optional_none`` and ``optional_some``, resp., in ``Expr`` and ``CasePat``.

.. _release-0-13-6:

0.13.6 - 2019-06-25
-------------------

DAML Assistant
~~~~~~~~~~~~~~

- Added ``--install-assistant`` flag to ``daml install`` command,
  changing the default behavior of ``daml install`` to install the assistant
  whenever we are installing a newer version of the SDK. Deprecated the
  ``--activate`` flag.
- Added ``--start-navigator``, ``--on-start``, and ``--wait-for-signal``
  options to ``daml start``, to make scripting and testing with the sandbox much easier.

DAML Studio
~~~~~~~~~~~

- Opening an already open scenario will now focus it rather than opening
  it in a new empty tab which is never updated with results.
- The selected view for scenario results (table or transaction) is now
  preserved when the scenario results are updated.
  See `#1675 <https://github.com/digital-asset/daml/issues/1675>`__.
- Goto definition now works on the export list of modules.
- Goto definition now works on types.

DAML-LF
~~~~~~~

- Rename ``TO_TEXT_CODE_POINTS`` and ``FROM_TEXT_CODE_POINTS`` to ``TEXT_FROM_CODE_POINTS`` and ``TEXT_TO_CODE_POINTS``, resp.

Dependencies
~~~~~~~~~~~~

- Protobuf has been upgraded to version 3.8.0. This
  also includes the protobuf-java library used as a dependency.

Ledger API
~~~~~~~~~~

- Added additional Ledger API integration tests to Ledger API Test Tool.

Java Bindings
~~~~~~~~~~~~~

- The artefact ``com.daml.ledger:bindings-java`` now has ``grpc-netty`` as dependency so that users don't need to explicitly add it.

DAML Integration Kit
~~~~~~~~~~~~~~~~~~~~

- Fixed a bug in the test tool that prevented users from running the tests.
  See `#1841 <https://github.com/digital-asset/daml/issues/1841>`__

Navigator
~~~~~~~~~

- Added support for SDK project configuration files. If you start Navigator with the SDK Assistant,
  Navigator will directly read the ``daml.yaml`` config file instead of the old Navigator config file.
  See `#1128 <https://github.com/digital-asset/daml/issues/1128>`__.

Docker Image
~~~~~~~~~~~~

- The daml-sdk docker images are now based on Alpine Linux.

.. _release-0-13-5:

0.13.5 - 2019-06-19
-------------------

Release Procedure
~~~~~~~~~~~~~~~~~

- Fixes to the CI/CD release procedure.
  See `#1755 <https://github.com/digital-asset/daml/issues/1755>__.`

Sandbox
~~~~~~~

- Introduced a new API for package management.
  See `#1311 <https://github.com/digital-asset/daml/issues/1311>`__.

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
- **DAML Integration Kit**: Alpha release of the kit for integrating your own ledger with DAML. See the DAML Integration Kit docs for how to try it out.
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
- **DAML Integration Kit**: Added documentation for the DAML Integration Kit. The docs explain what the DAML Integration Kit is, what state it is in, and how it is going to evolve.
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
