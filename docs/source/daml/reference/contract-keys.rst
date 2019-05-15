.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Contract keys
#############

Contract keys are an optional addition to templates. They let you specify a way of uniquely identifying contract instances, using the parameters to the template - similar to a primary key for a database.

You can use contract keys to stably refer to a contract, even through iterations of instances of it.

Here's an example of setting up a contract key for a bank account, to act as a bank account ID:

.. literalinclude:: ../code-snippets/ContractKeys.daml
   :language: daml
   :start-after: -- start contract key setup snippet
   :end-before: -- end contract key setup snippet

What can be a contract key
**************************

The key should either be a tuple or a record, and it **must** include every party that you specify as a ``maintainer`` (see `Specifying maintainers`_ below).

For example, with ``maintainer bank``, ``key (bank, number) : (Party, Text)`` is valid; but wouldn't be if you removed ``bank`` from the key.

It's best to use simple types for your keys like ``Text`` or ``Int``, rather than a list or more complex type.

Specifying maintainers
**********************

If you specify a contract key for a template, you must also specify a ``maintainer`` or maintainers, in a similar way to specifying signatories or observers. Maintainers are the parties that know about all of the keys that they are party to, and are used by the engine to guarantee uniqueness of contract keys.  The maintainers **must** be signatories or observers of the contract.

Keys are unique only to their maintainers. For example, say you have a key that you're using as the identifer for a ``BankAccount`` contract. You might have ``key (bank, accountId) : (Party, Text)``. When you create a new bank account, the contract key ensures that no-one else can have an account with the same ``accountID`` at that bank. But that doesn't apply to other banks: for a contract with a different bank as maintainer, you could happily re-use that ``accountID``.

When you're writing DAML models, the maintainers only matter since they affect authorization -- much like signatories and observers. You don't need to do anything to "maintain" the keys.

Checking of the keys is done automatically at execution time, by the DAML exeuction engine: if someone tries to create a new contract that duplicates an existing contract key, the execution engine will cause that creation to fail. 

Contract keys functions
***********************

Contract keys introduce several new functions.

``fetchByKey``
==============

``(fetchedContractId, fetchedContract) <- fetchByKey @ContractType contractKey``

Use ``fetchByKey`` to fetch the ID and data of the contract with the specified key. It is an alternative to the currently-used ``fetch``.

It returns a tuple of the ID and the contract object (containing all its data). 

You need authorization from **at least one** of the maintainers to run ``fetchByKey``. A maintainer can authorize by being a signatory, or by submitting the command/being a controller for the choice.

``fetchByKey`` fails and aborts the transaction if:

- you don't have sufficient authorization
- you're not a stakeholder of the contract you're trying to fetch

This means that if it fails, it doesn't guarantee that a contract with that key doesn't exist, just that you can't see one.

Because the type is ambiguous, when calling you need to specify what you expect with ``@`` and the type, for example ``@MyTemplateType``.

``lookupByKey``
===============

``contractId <- lookupByKey @ContractType contractKey``

Use ``lookupByKey`` to check whether a contract with the specified key exists. If it does exist, ``lookupByKey`` returns the ``Some contractId``, where ``contractId`` is the ID of the contract; otherwise, it returns ``None``.

You need authorization from **all** of the maintainers to run ``lookupByKey``, and it can only be submitted by one of the maintainers.

If the lookup fails (ie returns ``None``), this guarantees that no contract has this key.

Unlike ``fetchByKey``, the transaction **does not fail** if a contract with the key doesn't exist: instead, ``lookupByKey`` just returns ``None``.

To get the data from the contract once you've confirmed it exists, you'll still need to use ``fetch``.

Because the type is ambiguous, when calling you need to specify what you expect with ``@`` and the type, for example ``@MyTemplateType``.

``exerciseByKey``
=================

``exerciseByKey @ContractType contractKey``

Use ``exerciseByKey`` to exercise a choice on a contract identified by its ``key`` (compared to ``exercise``, which lets you exercise a contract identified by its ``ContractId``). To run ``exerciseByKey`` you need authorization from the controllers of the choice and at least one of the key maintainers.

Because the type is ambiguous, when calling you need to specify what you expect with ``@`` and the type, for example ``@MyTemplateType``.

Error messages
**************

If you don't include the ``maintainer`` in your ``key``, you'll see the following error::

   Failure to process DAML program, this feature is not currently supported.
   Unbound reference to this in maintainer with evar.
