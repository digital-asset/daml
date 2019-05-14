.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Contract keys
#############

Contract keys are an optional addition to templates. They let you specify a way of uniquely identifying contract instances: that can be a single parameter to the template or a combination of parameters (eg, in a tuple).

You can use contract keys to stably refer to a contract, even through iterations of instances of it.

Here's an example of setting up a contract key for a bank account, to act as a bank account ID:

.. literalinclude:: ../code-snippets/ContractKeys.daml
   :language: daml
   :start-after: -- start contract key setup snippet
   :end-before: -- end contract key setup snippet

What can be a contract key
**************************

What you can specify is pretty flexible - you will probably want to set up a data type for your key, which is either a tuple or a record.

However, the contract key *must* include every party that you specify as a ``maintainer``. For example, with ``maintainer bank``, ``key (bank, number) : AccountKey`` is valid; but wouldn't be if you removed ``bank`` from the key.

Specifying maintainers
**********************

If you specify a contract key for a template, you must also specify ``maintainer`` (s), in a similar way to specifying signatories or observers.

Maintainers are the parties that know about all of the keys that they are party to, and so it's their responsibility to guarantee uniqueness of contract keys. The maintainers **must** be signatories or observers of the contract.

When you're writing DAML models, the maintainers only matter since they affect authorization -- much like signatories and observers. You don't need to do anything to "maintain" the keys.

Checking of the keys is done automatically at execution time, by the DAML exeuction engine: if someone tries to create a new contract that duplicates an existing contract key, the execution engine will cause that creation to fail. 

Contract keys functions
***********************

Contract keys introduce several new functions.

``fetchByKey``
==============

Use ``fetchByKey`` to fetch the ID and data of the contract with the specified key. It is an alternative to the currently-used ``fetch``.

It returns a tuple of the ID and the contract object (containing all its data). 

You need authorization from **at least one** of the maintainers to run ``fetchByKey``. A maintainer can authorize by being a signatory, or by submitting the command/being a controller for the choice.

``fetchByKey`` fails and aborts the transaction if:

- you don't have sufficient authorization
- you're not a stakeholder of the contract you're trying to fetch

This means that if it fails, it doesn't guarantee that a contract with that key doesn't exist, just that you can't see one.

TODO code example.

``lookupByKey``
===============

Use ``lookupByKey`` to check whether a contract with the specified key exists. If it does exist, ``lookupByKey`` returns the ``ContractId`` of the contract; otherwise, it returns ``None``.

You need authorization from **all** of the maintainers to run ``lookupByKey``, and it can only be submitted by one of the maintainers. TODO what the failure will look like.

If the lookup fails (ie returns ``None``), this guarantees that no contract has this key.

To get the data from the contract once you've confirmed it exists, you'll still need to use ``fetch``.

TODO code example.

``exerciseByKey``
=================

Use ``exerciseByKey`` to exercise a choice on a contract identified by its ``key`` (compared to ``exercise``, which lets you exercise a contract identified by its ``ContractId``).

Error messages
**************

If you don't include the ``maintainer``s in your ``key``, you'll see the following error::

   Failure to process DAML program, this feature is not currently supported.
   Unbound reference to this in maintainer with evar.
