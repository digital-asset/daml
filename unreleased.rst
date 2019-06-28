.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------
- [DAML Standard Library] **BREAKING CHANGE**: Remove the deprecated modules ``DA.Map``, ``DA.Set``, ``DA.Experimental.Map`` and ``DA.Experimental.Set``. Please use ``DA.Next.Map`` and ``DA.Next.Set`` instead.
- [Sandbox] Fixed an issue when CompletionService returns offsets having inclusive semantics when used for re-subscription. 
  See `#1932 <https://github.com/digital-asset/daml/pull/1932>`__.
  