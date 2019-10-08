.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

HEAD — ongoing
--------------

+ [Java Bindings] Add helper to prepare transformer for ``Bot.wire``. See `issue #3097 <https://github.com/digital-asset/daml/issues/3097>`_.
+ [SQL Extractor] The format used for storing Optional and Map values found in contracts
  as JSON has been replaced with :doc:`/json-api/lf-value-specification`.  See `issue
  #3066 <https://github.com/digital-asset/daml/issues/3066>`_ for specifics.
[Ledger] The ledger api index server starts only after the indexer has finished initializing the database.
+ [DAML Standard Library] Add ``DA.Action.State`` module containing a ``State`` action that
  can be used for computations that modify a state variable.
