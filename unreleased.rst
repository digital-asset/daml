.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [JSON API] **BREAKING CHANGE** Flattening the output of the ``/contracts/search`` endpoint.
  The endpoint returns ``ActiveContract`` objects without ``GetActiveContractsResponse`` wrappers.
  See `issue #2987 <https://github.com/digital-asset/daml/pull/2987>`_.
