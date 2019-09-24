.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [JSON API - Experimental] Flattening the output of the ``/contracts/search`` endpoint.
  The endpoint returns ``ActiveContract`` objects without ``GetActiveContractsResponse`` wrappers.
  See `issue #2987 <https://github.com/digital-asset/daml/pull/2987>`_.
- [DAML Assistant] ``daml start`` now supports ``--sandbox-option=opt``, ``--navigator-option=opt``
  and ``--json-api-option=opt`` to pass additional option to sandbox/navigator/json-api.
  These flags can be specified multiple times.
