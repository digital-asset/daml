.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _contractkeys:

Reference: Contract Keys
########################

Contract keys are an optional addition to templates. They let you specify a way of uniquely identifying contracts, using the parameters to the template - similar to a primary key for a database.

You can use contract keys to stably refer to a contract, even through iterations of instances of it.

Here's an example of setting up a contract key for a bank account, to act as a bank account ID:

.. literalinclude:: ../code-snippets/Account.daml
   :language: daml
   :start-after: -- start contract key setup snippet
   :end-before: -- end contract key setup snippet

What Can Be a Contract Key
**************************

The key can be an arbitrary serializable expression that does **not** contain contract IDs. However, it **must** include every party that you want to use as a ``maintainer`` (see `Specify Maintainers`_ below).

It's best to use simple types for your keys like ``Text`` or ``Int``, rather than a list or more complex type.

Specify Maintainers
*******************

If you specify a contract key for a template, you must also specify a ``maintainer`` or maintainers, in a similar way to specifying signatories or observers. The maintainers "own" the key in the same way the signatories "own" a contract. Just like signatories of contracts prevent double spends or use of false contract data, maintainers of keys prevent double allocation or incorrect lookups. Since the key is part of the contract, the maintainers **must** be signatories of the contract. However, maintainers are computed from the ``key`` instead of the template arguments. In the example above, the ``bank`` is ultimately the maintainer of the key.

Uniqueness of keys is guaranteed per template. Since multiple templates may use the same key type, some key-related functions must be annotated using the ``@ContractType`` as shown in the examples below.

When you are writing Daml models, the maintainers matter since they affect authorization -- much like signatories and observers. You don't need to do anything to "maintain" the keys. In the above example, it is guaranteed that there can only be one ``Account`` with a given ``number`` at a given ``bank``.

Checking of the keys is done automatically at execution time, by the Daml execution engine: if someone tries to create a new contract that duplicates an existing contract key, the execution engine will cause that creation to fail.

Contract Lookups
****************

The primary purpose of contract keys is to provide a stable, and possibly meaningful, identifier that can be used in Daml to fetch contracts. There are two functions to perform such lookups: :ref:`fetchbykey` and :ref:`lookupbykey`. Both types of lookup are performed at interpretation time on the submitting Participant Node, on a best-effort basis. Currently, that best-effort means lookups only return contracts if the submitting Party is a stakeholder of that contract.

In particular, the above means that if multiple commands are submitted simultaneously, all using contract lookups to find and consume a given contract, there will be contention between these commands, and at most one will succeed.

Limiting key usage to stakeholders also means that keys cannot be used to access a divulged contract, i.e. there can be cases where `fetch` succeeds and `fetchByKey` does not. See the example at the end of this section for details.

.. _fetchbykey:

fetchByKey
==========

``(fetchedContractId, fetchedContract) <- fetchByKey @ContractType contractKey``

Use ``fetchByKey`` to fetch the ID and data of the contract with the specified key. It is an alternative to ``fetch`` and behaves the same in most ways.

It returns a tuple of the ID and the contract object (containing all its data).

Like ``fetch``, ``fetchByKey`` needs to be authorized by at least one stakeholder.

``fetchByKey`` fails and aborts the transaction if:

- The submitting Party is not a stakeholder on a contract with the given key, or
- A contract was found, but the ``fetchByKey`` violates the authorization rule, meaning no stakeholder authorized the ``fetch``.

This means that if it fails, it doesn't guarantee that a contract with that key doesn't exist, just that the submitting Party doesn't know about it, or there are issues with authorization.

.. _visiblebykey:

visibleByKey
============

``boolean <- visibleByKey @ContractType contractKey``

Use ``visibleByKey`` to check whether you can see an active contract for the given key with the current authorizations. If the contract exists and you have permission to see it, returns ``True``, otherwise returns ``False``.

To clarify, ignoring contention:

1. ``visibleByKey`` will return ``True`` if all of these are true: there exists a contract for the given key, the submitter is a stakeholder on that contract, and at the point of call we have the authorization of **all** of the maintainers of the key.
2. ``visibleByKey`` will return ``False`` if all of those are true: there is no contract for the given key, and at the point of call we have authorization from **all** the maintainers of the key.
3. ``visibleByKey`` will abort the transaction at interpretation time if, at the point of call, we are missing the authorization from any one maintainer of the key.
4. ``visibleByKey`` will fail at validation time (after returning ``False`` at interpretation time) if all of these are true: at the point of call, we have the authorization of **all** the maintainers, and a valid contract exists for the given key, but the submitter is not a stakeholder on that contract.

While it may at first seem too restrictive to require **all** maintainers to authorize the call, this is actually required in order to validate negative lookups. In the positive case, when you can see the contract, it's easy for the transaction to mention which contract it found, and therefore for validators to check that this contract does indeed exist, and is active as of the time of executing the transaction.

For the negative case, however, the transaction submitted for execution cannot say *which* contract it has not found (as, by definition, it has not found it, and it may not even exist). Still, validators have to be able to reproduce the result of not finding the contract, and therefore they need to be able to look for it, which means having the authorization to ask the maintainers about it.

.. _lookupbykey:

lookupByKey
===========

``optionalContractId <- lookupByKey @ContractType contractKey``

Use ``lookupByKey`` to check whether a contract with the specified key exists. If it does exist, ``lookupByKey`` returns the ``Some contractId``, where ``contractId`` is the ID of the contract; otherwise, it returns ``None``.

``lookupByKey`` is conceptually equivalent to

.. code-block:: daml

   lookupByKey : forall c k. (HasFetchByKey c k) => k -> Update (Optional (ContractId c))
   lookupByKey k = do
     visible <- visibleByKey @c k
     if visible then do
       (contractId, _ignoredContract) <- fetchByKey @c k
       return $ Some contractId
     else
       return None

Therefore, ``lookupByKey`` needs all the same authorizations as `visibleByKey`, for the same reasons, and fails in the same cases.

To get the data from the contract once you've confirmed it exists, you'll still need to use ``fetch``.

exerciseByKey
*************

``exerciseByKey @ContractType contractKey``

Use ``exerciseByKey`` to exercise a choice on a contract identified by its ``key`` (compared to ``exercise``, which lets you exercise a contract identified by its ``ContractId``). To run ``exerciseByKey`` you need authorization from the controllers of the choice and at least one stakeholder. This is equivalent to the authorization needed to do a ``fetchByKey`` followed by an ``exercise``.

Example
*******

A complete example of possible success and failure scenarios of `fetchByKey` and `lookupByKey` is shown below.


.. literalinclude:: ../code-snippets/Keys.daml
   :language: daml
