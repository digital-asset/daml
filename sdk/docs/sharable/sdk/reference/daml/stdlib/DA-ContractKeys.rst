.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-contractkeys-19667:

DA.ContractKeys
===============

Note\: Docs TODO (https\://github\.com/digital\-asset/daml/issues/22673)

Functions
---------

.. _function-da-internal-template-functions-lookupnbykey-93090:

`lookupNByKey <function-da-internal-template-functions-lookupnbykey-93090_>`_
  \: :ref:`HasLookupNByKey <class-da-internal-template-functions-haslookupnbykey-35508>` t k \=\> :ref:`Int <type-ghc-types-int-37261>` \-\> k \-\> :ref:`Update <type-da-internal-lf-update-68072>` \[(:ref:`ContractId <type-da-internal-lf-contractid-95282>` t, t)\]

  Look up up to n contracts associated with the passed key\. Contracts created
  within a transaction are returned first (in recency order), then disclosed
  contracts, then nonlocal contracts\.

  You must pass the ``t`` using an explicit type application\. For instance, if
  you want to query 3 contracts of template ``Account`` given by its key ``k``, you
  must call \`lookupNByKey @Account 3 k\.

.. _function-da-internal-template-functions-lookupallbykey-2641:

`lookupAllByKey <function-da-internal-template-functions-lookupallbykey-2641_>`_
  \: :ref:`HasLookupNByKey <class-da-internal-template-functions-haslookupnbykey-35508>` t k \=\> k \-\> :ref:`Update <type-da-internal-lf-update-68072>` \[(:ref:`ContractId <type-da-internal-lf-contractid-95282>` t, t)\]

  Shorthand for ``lookupNByKey`` with n larger than the amount of contracts that
  can exist for a key (1000000), therefore returning all contracts
