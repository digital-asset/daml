.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

   
Contract keys
#############

Contract keys are an optional addition to templates. They let you specify a way of uniquely identifying contract instances, using the parameters to the template - similar to a primary key for a database.

You can use contract keys to stably refer to a contract, even through iterations of instances of it.

Here's an example of setting up a contract key for a bank account, to act as a bank account ID:

.. literalinclude:: ../code-snippets/Account.daml
   :language: daml
   :start-after: -- start contract key setup snippet
   :end-before: -- end contract key setup snippet

What can be a contract key
**************************

The key can be an arbitrary serializable expression that does **not** contain contract IDs. However, it **must** include every party that you want to use as a ``maintainer`` (see `Specifying maintainers`_ below).

It's best to use simple types for your keys like ``Text`` or ``Int``, rather than a list or more complex type.

Specifying maintainers
**********************

If you specify a contract key for a template, you must also specify a ``maintainer`` or maintainers, in a similar way to specifying signatories or observers. The maintainers "own" the key in the same way the signatories "own" a contract. Just like signatories of contracts prevent double spends or use of false contract data, maintainers of keys prevent double allocation or incorrect lookups. Since the key is part of the contract, the maintainers **must** be signatories of the contract. However, maintainers are computed from the ``key`` instead of the template arguments.  In the example above, the ``bank`` is ultimately the maintainer of the key. 

Uniqueness of keys is guaranteed per template. Since multiple templates may use the same key type, some key-related function must be annotated using the ``@ContractType`` as shown in the examples below.

When you are writing DAML models, the maintainers matter since they affect authorization -- much like signatories and observers. You don't need to do anything to "maintain" the keys. In the above example, it is guaranteed, there there can only be one ``Account`` with a given ``number`` at a given ``bank``.

Checking of the keys is done automatically at execution time, by the DAML exeuction engine: if someone tries to create a new contract that duplicates an existing contract key, the execution engine will cause that creation to fail. 

Contract Lookups
****************

The primary purpose of contract keys is to provide a stable, and possibly meaningful, identifier for contracts that can be used in DAML to fetch contracts. There are two functions to perform such lookups: :ref:`fetchbykey` and :ref:`lookupbykey`. Both types of lookup are performed at interpretation time on the submitting Partipant Node, on a best-effort basis. Currently, that best-effort means lookups only return contracts if the submitting Party is a stakeholder of that contract.

In particular, the above means that if multiple commands are submitted simultaneously, all using contract lookups to find and consume a given contract, there will be contention between these commands, and at most one will succeed.

.. _fetchbykey:

fetchByKey
==========

``(fetchedContractId, fetchedContract) <- fetchByKey @ContractType contractKey``

Use ``fetchByKey`` to fetch the ID and data of the contract with the specified key. It is an alternative to ``fetch`` and behaves the same in most ways. 

It returns a tuple of the ID and the contract object (containing all its data).

Like ``fetch``, ``fetchByKey`` needs to be authorized by at least one stakeholder.

``fetchByKey`` fails and aborts the transaction if:

- The submitting Party is not a stakeholder on a contract with the given key, or
- A contract was found, but the ``fetchByKey`` violates the authorization rule, meaning no stakeholder authorized the ``fetch``..

This means that if it fails, it doesn't guarantee that a contract with that key doesn't exist, just that the submitting Party doesn't know about it, or there are issues with authorization.

.. _lookupbykey:

lookupByKey
===========

``optionalContractId <- lookupByKey @ContractType contractKey``

Use ``lookupByKey`` to check whether a contract with the specified key exists. If it does exist, ``lookupByKey`` returns the ``Some contractId``, where ``contractId`` is the ID of the contract; otherwise, it returns ``None``.

``lookupByKey`` needs to be authorized by **all** maintainers of the contract you are trying to lookup. The reason for that is denial of service protection. Without this restriction, a malicious Party could legitimately request another Party to validate any number of negative contract key lookups without that other Party ever having signed anything.

Unlike ``fetchByKey``, the transaction **does not fail** if no contract with the given key is found. Instead, ``lookupByKey`` just returns ``None``. However, that does not guarantee that no such contract exists. The submitting Party may simply not know about it, in which case the transaction will be rejected during validation.

``lookupByKey`` fails and aborts the transaction if:

- Authorization from at least one maintainer is missing. This check fails at interpretation time.
- The lookup is incorrect. This can happen either due to contention, or because the submitter didn't know of the contract. This check fails at validation time.


To get the data from the contract once you've confirmed it exists, you'll still need to use ``fetch``.

exerciseByKey
*************

``exerciseByKey @ContractType contractKey``

Use ``exerciseByKey`` to exercise a choice on a contract identified by its ``key`` (compared to ``exercise``, which lets you exercise a contract identified by its ``ContractId``). To run ``exerciseByKey`` you need authorization from the controllers of the choice and at least one stakeholder. This is equivalent to the authorization needed to fo a ``fetchByKey`` followed by an ``exercise``.
