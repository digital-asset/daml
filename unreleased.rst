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
