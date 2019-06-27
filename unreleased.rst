.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

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
