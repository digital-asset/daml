.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [JSON API - Experimental] Returning archived and active/created contracts from ``/command/exercise``
+ [SDK] Bundle the ``daml-trigger`` package. Note, this package is experimental and will change.
+ [SDK] Releases can now bundle additional libraries with the SDK in ``$DAML_SDK/daml-libs``. You
  can refer to them in your ``daml.yaml`` file by listing the package name without ``.dar``
  extension. See `issue #2979 <https://github.com/digital-asset/daml/issues/2979>`_.
+ [JSON API - Experimental] Returning archived and active contracts from ``/command/exercise``
  enpoint. See `issue #2925 <https://github.com/digital-asset/daml/issues/2925>`_.
+ [JSON API - Experimental] Flattening the output of the ``/contracts/search`` endpoint.
  The endpoint returns ``ActiveContract`` objects without ``GetActiveContractsResponse`` wrappers.
  See `issue #2987 <https://github.com/digital-asset/daml/pull/2987>`_.
- [DAML Assistant] ``daml start`` now supports ``--sandbox-option=opt``, ``--navigator-option=opt``
  and ``--json-api-option=opt`` to pass additional option to sandbox/navigator/json-api.
  These flags can be specified multiple times.
- [DAML Studio] ``damlc ide`` now also supports a ``--target`` option.
  The easiest way to specify this is the ``build-options`` field in ``daml.yaml``.
- [Ledger]
  Improve SQL backend performance by eliminating extra queries to the database.
- [DAML Tool - Visual]
  Adding `daml damlc visual-web` command. visual-command generates webpage with `d3 <https://d3js.org>`_ network.
+ [DAML Ledger Integration Kit] The transaction service is now fully tested.
- [DAML Compiler] Fix a problem where constraints of the form ``Template (Foo t)`` caused the compiler to suggest enabling the ``UndecidableInstances`` language extension.
- [Security] Document how to verify the signature on release tarballs.
+ [DAML Ledger Integration Kit] The TTL for commands is now read from the configuration service.
+ [DAML Ledger Integration Kit] The contract key tests now live under a single test suite and are multi-node aware.
- [DAML Compiler] Generic template instantiations like ``template instance IouProposal = Proposal Iou`` now generate a type synonym ``type IouProposal = Proposal Iou`` that can be used in DAML. Before, they generated a ``newtype``, which cannot be used anymore.
- [DAML Compiler] Fixed a bug where ``damlc build`` sometimes did not find modules during typechecking
  even if they were present during parallel compilations.
- [Ledger] The ledger api server now stores divulged, otherwise unknown contracts.
