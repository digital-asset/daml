.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

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
