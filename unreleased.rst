.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

- [Sandbox] Added `--log-level` command line flag.
- [Navigator] Contract details now show signatories and observers.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.
- [Scala Bindings] Reflect addition of signatories and observers to the bindings.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.
- [Java Codegen] Generated code supports signatories and observers as exposed by the bindings.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.
- [Java Bindings] Reflect addition of signatories and observers to the bindings.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.
- [Ledger API] Expose signatories and observers for a contract in ``CreatedEvent``.
  See `#1269 <https://github.com/digital-asset/daml/issues/1269>`__.
- [DAML Compiler] **BREAKING CHANGE**: Drop support for DAML-LF 1.4. Compiling to DAML-LF 1.5 should work without any code changes, although we highly recommend not specifying a target DAML-LF version at all. (The ledger server still supports DAML-LF 1.4.)
- [Sandbox] Made the archive CLI arguments optional. 
  See `#1905 <https://github.com/digital-asset/daml/issues/1905>`__.
- [DAML-LF] **BREAKING CHANGE**: Specify pretty C# namespaces in archive protos. C# bindings will end up in a different namespace than the default one.
  See `#1900 <https://github.com/digital-asset/daml/issues/1900>`__.
- [Ledger API] **BREAKING CHANGE**: Specify pretty C# namespaces in ledger api protos. C# bindings will end up in a different namespace than the default one.
  See `#1901 <https://github.com/digital-asset/daml/issues/1901>`__.
- [Ledger API] Added new CLI flags ``--stable-party-identifiers`` and
  ``--stable-command-identifiers`` to the :doc:`Ledger API Test Tool
  </tools/ledger-api-test-tool/index>` to allow disabling randomization of party
  and command identifiers. It is useful for testing of ledgers which are
  configured with a predefined static set of parties.
