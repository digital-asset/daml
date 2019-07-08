.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [daml assistant] Fix VSCode path for use if not already in PATH on mac
- [Sandbox] Fixing an issue around handling passTime in scenario loader
  See `#1953 <https://github.com/digital-asset/daml/issues/1953>`__.
- [DAML Studio] DAML Studio now displays a “Processing” indicator on the bottom
  left while the IDE is doing work in the background.
- [Sandbox] Remembering already loaded packages after reset
  See `#1979 <https://github.com/digital-asset/daml/issues/1953>`__.
  
- [DAML-LF] Fixed regression that produced an invalid daml-lf-archive artefact. See `#2058 <https://github.com/digital-asset/daml/issues/2058>`__.
- [Sandbox] The completion stream method of the command completion service uses the ledger end as a default value for the offset. See `#1913 <https://github.com/digital-asset/daml/issues/1913>`__.
- [Java bindings] Added overloads to the Java bindings ``CompletionStreamRequest`` constructor and the ``CommandCompletionClient`` to accept a request without an explicit ledger offset. See `#1913 <https://github.com/digital-asset/daml/issues/1913>`__.
- [Java bindings] **DEPRECATION**: the ``CompletionStreamRequest#getOffset`` method is deprecated in favor of the non-nullable ``CompletionStreamRequest#getLedgerOffset``. See `#1913 <https://github.com/digital-asset/daml/issues/1913>`__.
- [Scala bindings] Contract keys are exposed on CreatedEvent. See `#1681 <https://github.com/digital-asset/daml/issues/1681>`__.
- [Navigator] Contract keys are show in the contract details page. See `#1681 <https://github.com/digital-asset/daml/issues/1681>`__.
- [DAML Standard Library] **BREAKING CHANGE**: Remove the deprecated modules ``DA.Map``, ``DA.Set``, ``DA.Experimental.Map`` and ``DA.Experimental.Set``. Please use ``DA.Next.Map`` and ``DA.Next.Set`` instead.
- [Sandbox] Fixed an issue when CompletionService returns offsets having inclusive semantics when used for re-subscription.
  See `#1932 <https://github.com/digital-asset/daml/pull/1932>`__.

- [DAML Compiler] The default output path for all artifacts is now in the ``.daml`` directory.
  In particular, the default output path for .dar files in ``daml build`` is now
  ``.daml/dist/<projectname>.dar``.

- [DAML Studio] DAML Studio is now published as an extension in the Visual Studio Code
  marketplace. The ``daml studio`` command will now install the published extension by
  default, but will revert to the extension bundled with the DAML SDK if installation
  fails. You can get the old default behavior of always using the bundled extension
  by running ``daml studio --replace=newer`` or ``daml studio --replace=always`` instead.
- [DAML Studio] You can now configure the gRPC message size limit in
  ``daml.yaml`` via ``scenario-service: {"grpc-max-message-size": 1000000}``.
  This will set the limit to 1000000 bytes. This should
  only be necessary for very large projects.
- [Sandbox] DAML-LF packages used by the sandbox are now stored in Postgres,
  allowing users to resume a Postgres sandbox ledger without having to again
  specify all packages through the CLI.
  See `#1929 <https://github.com/digital-asset/daml/issues/1929>`__.
- [DAML Studio] You can now configure the gRPC timeout
  ``daml.yaml`` via ``scenario-service: {"grpc-timeout": 42}``.
  This option will set the timeout to 42 seconds. You should
  only need to set this option for very large projects.
- [DAML Standard Library] Add ``Sum`` and ``Product`` newtypes that
  provide ``Monoid`` instances based on the ``Additive`` and ``Multiplicative``
  instances of the underlying type.
- [DAML Standard Library] Add ``Min`` and ``Max`` newtypes that
  provide ``Semigroup`` instances based ``min`` and ``max``.
- [DAML Integration Kit] Make DivulgenceIT properly work when run via the Ledger API Test Tool.

- [DAML-LF] The DAML-LF developement version (``1.dev``) includes a new, breaking restriction
  regarding contract key lookups. In short, when looking up or fetching a key,
  the transaction submitter must be one of the key maintainers.

  Note that this change is not breaking since the compiler does not produce DAML-LF
  ``1.dev`` by default. However it will be a breaking change once this restriction
  makes it into DAML-LF ``1.6`` and once DAML-LF ``1.6`` becomes the default.
- [DAML Integration Toolkit] The submission service shuts down its ExecutorService upon exit to ensure a smooth shutdown.
- [Sandbox] Added `--log-level` command line flag.
- [Sandbox] Added ``--log-level`` command line flag.
- [Ledger API] Added new CLI flags ``--stable-party-identifiers`` and
  ``--stable-command-identifiers`` to the :doc:`Ledger API Test Tool
  </tools/ledger-api-test-tool/index>` to allow disabling randomization of party
  and command identifiers. It is useful for testing of ledgers which are
  configured with a predefined static set of parties.

- [DAML-LF]: Release version 1.6. This versions provides:

  + ``enum`` types. See issue
    `#105 <https://github.com/digital-asset/daml/issues/105>`__ and DAML-LF 1
    specification for more details.

  + new builtins for (un)packing strings. See
    `#16 <https://github.com/digital-asset/daml/issues/16>`__.

  + intern package IDs. See
    `#1614 <https://github.com/digital-asset/daml/pull/1614>`__.

  + **Breaking Change** Restrict contract key lookups for DAML-LF 1.6. In
    short, when looking up or fetching a key, the transaction submitter
    must be one of the key maintainers. See
    `#1866 <https://github.com/digital-asset/daml/issues/1866>`__.

- [DAML Compiler]: Add support for DAML-LF 1.6. In particular:

  + **Breaking Change** Add support for ``enum`` types. DAML variants type that
    look like enumerations (i.e., those variants without type parameters and
    without argument) are compiled to DAML-LF ``enum`` type when daml-lf 1.6
    target is selected. For instance the daml type declaration of the form::

      data Color = Red | Green | Blue

    will produce a DAML-LF ``enum`` type instead of DAML-LF ``variant`` type.

  + Add ``DA.Text.toCodePoints`` and ``DA.Text.fromCodePoints`` primitives to
    (un)pack strings.

  + Add support for daml-lf intern package IDs.

- [DAML Compiler]: Make DAML-LF 1.6 the default output.

- [Ledger API] Add support for ``enum`` types. Simple ``variant`` types will
  be replaced by ``enum`` types.

- [Java Codegen]: Add support for ``enum`` types. Simple ``variant`` types will
  be replaced by ``enum`` types.

- [Scala Codegen]: Add support for ``enum`` types.  Simple ``variant`` types will
  be replaced by ``enum`` types.

- [Navigator]: Add support for ``enum`` types.

- [Extractor]: Add support for ``enum`` types.
