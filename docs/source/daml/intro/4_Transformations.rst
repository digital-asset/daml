.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

4 Transforming data using choices
=================================

In the example in :ref:`contract_keys` the accountant party wanted to change some data on a contract. They did so by archiving the contract and re-creating it with the updated data. That works because the accountant is the sole signatory on the ``Account`` contract defined there.

But what if the accountant wanted to allow the bank to change their own telephone number? Or what if the owner of a ``CashBalance`` should be able to transfer ownership to someone else?

In this section you will learn about how to define simple data transformations using *choices* and how to delegate the right to *exercise* these choices to other parties.

Choices as methods
------------------

If you think of templates as classes and contracts as objects, where are the methods?

Take as an example a ``Contact`` contract on which the contact owner wants to be able to change the telephone number, just like on the ``Account`` in :ref:`contract_keys`. Rather than requiring them to manually look up the contract, archive the old one and create a new one, you can provide them a convenience method on ``Contact``:

.. literalinclude:: daml/daml-intro-4/Contact.daml
  :language: daml
  :start-after: -- CHOICE_BEGIN
  :end-before: -- CHOICE_END

The above defines a *choice* called ``UpdateTelephone``. Choices are part of a contract template. They're permissioned functions that result in an ``Update``. Using choices, authority can be passed around, allowing the construction of complex transactions.

Let's unpack the code snippet above:

- The first line, ``controller owner can`` says that the following choices are *controlled* by ``owner``, meaning ``owner`` is the only party that is allowed to *exercise* them. The line starts a new block in which multiple choices can be defined.
- ``UpdateTelephone`` is the name of a choice. It starts a new block in which that choice is defined.
- ``: ContractId Contact`` is the return type of the choice.

  This particular choice archives the current ``Contact``, and creates a new one. What it returns is a reference to the new contract, in the form of a ``ContractId Contact``
- The following ``with`` block is that of a record. Just like with templates, in the background, a new record type is declared: ``data UpdateTelephone = UpdateTelephone with``
- The ``do`` starts a block defining the action the choice should perform when exercised. In this case a new ``Contact`` is created.
- The new ``Contact`` is created using ``this with``. ``this`` is a special value available within the ``where`` block of templates and takes the value of the current contract's arguments.

There is nothing here explicitly saying that the current ``Contact`` should be archived. That's because choices are *consuming* by default. That means when the above choice is exercised on a contract, that contract is archived.

If you paid a lot of attention in :doc:`3_Data`, you may have noticed that the ``create`` statement returns an ``Update (ContractId Contact)``, not a ``ContractId Contact``. As a ``do`` block always returns the value of the last statement within it, the whole ``do`` block returns an ``Update``, but the return type on the choice is just a ``ContractId Contact``. This is a convenience. Choices *always* return an ``Update`` so for readability it's omitted on the type declaration of a choice.

Now to exercise the new choice in a scenario:

.. literalinclude:: daml/daml-intro-4/Contact.daml
  :language: daml
  :start-after: -- CHOICE_TEST_BEGIN
  :end-before: -- CHOICE_TEST_END

You exercise choices using the ``exercise`` function, which takes a ``ContractId a``, and a value of type ``c``, where ``c`` is a choice on template ``a``. Since ``c`` is just a record, you can also just fill in the choice parameters using the ``with`` syntax you are already familiar with.

``exercise`` returns an ``Update r`` where ``r`` is the return type specified on the choice, allowing the new ``ContractId Contact`` to be stored in the variable ``new_contactCid``.

Choices as delegation
---------------------

Up to this point all the contracts only involved one party. ``party`` may have been stored as ``Party`` field in the above, which suggests they are actors on the ledger, but they couldn't see the contracts, nor change them in any way. It would be reasonable for the party for which a ``Contact`` is stored to be able to update their own address and telephone number. In other words, the ``owner`` of a ``Contact`` should be able to *delegate* the right to perform a certain kind of data transformation to ``party``.

The below demonstrates this using an ``UpdateAddress`` choice and corresponding extension of the scenario:

.. literalinclude:: daml/daml-intro-4/Contact.daml
  :language: daml
  :start-after: -- DELEGATION_BEGIN
  :end-before: -- DELEGATION_END

.. literalinclude:: daml/daml-intro-4/Contact.daml
  :language: daml
  :start-after: -- DELEGATION_TEST_BEGIN
  :end-before: -- DELEGATION_TEST_END

If you open the scenario view in the IDE, you will notice that Bob sees the ``Contact``. Controllers specified via ``controller c can`` syntax become *observers* of the contract. More on *observers* later, but in short, they get to see any changes to the contract.

.. _choices:

Choices in the Ledger Model
---------------------------

In :doc:`1_Token` you learned about the high-level structure of a DAML ledger. With choices and the `exercise` function, you have the next important ingredient to understand the structure of the ledger and transactions.

A *transaction* is a list of *actions*, and there are just four kinds of action: ``create``, ``exercise``, ``fetch`` and ``key assertion``.

- A ``create`` action creates a new contract with the given arguments and sets its status to *active*.
- A ``fetch`` action checks the existence and activeness of a contract.
- An ``exercise`` action exercises a choice on a contract resulting in a transaction (list of sub-actions) called the *consequences*. Exercises come in two kinds called ``consuming`` and ``nonconsuming``. ``consuming`` is the default kind and changes the contract's status from *active* to *archived*.
- A ``key assertion`` records the assertion that the given contract key (see :ref:`contract_keys`) is not assigned to any active contract on the ledger.

Each action can be visualized as a tree, where the action is the root node, and its children are its consequences. Every consequence may have further consequences. As ``fetch``, ``create`` and ``key assertion`` actions have no consequences, they are always leaf nodes. You can see the actions and their consequences in the transaction view of the above scenario:

.. code-block:: none

  Transactions:
    TX #0 1970-01-01T00:00:00Z (Contact:43:17)
    #0:0
    │   consumed by: #2:0
    │   referenced by #2:0
    │   known to (since): 'Alice' (#0), 'Bob' (#0)
    └─> create Contact:Contact
        with
          owner = 'Alice'; party = 'Bob'; address = "1 Bobstreet"; telephone = "012 345 6789"

    TX #1 1970-01-01T00:00:00Z
      mustFailAt 'Bob' (Contact:52:3)

    TX #2 1970-01-01T00:00:00Z (Contact:56:22)
    #2:0
    │   known to (since): 'Alice' (#2), 'Bob' (#2)
    └─> 'Alice' exercises UpdateTelephone on #0:0 (Contact:Contact)
                with
                  newTelephone = "098 7654 321"
        children:
        #2:1
        │   consumed by: #4:0
        │   referenced by #3:0, #4:0
        │   known to (since): 'Alice' (#2), 'Bob' (#2)
        └─> create Contact:Contact
            with
              owner = 'Alice'; party = 'Bob'; address = "1 Bobstreet"; telephone = "098 7654 321"

    TX #3 1970-01-01T00:00:00Z (Contact:60:3)
    #3:0
    └─> fetch #2:1 (Contact:Contact)

    TX #4 1970-01-01T00:00:00Z (Contact:66:22)
    #4:0
    │   known to (since): 'Alice' (#4), 'Bob' (#4)
    └─> 'Bob' exercises UpdateAddress on #2:1 (Contact:Contact)
              with
                newAddress = "1-10 Bobstreet"
        children:
        #4:1
        │   referenced by #5:0
        │   known to (since): 'Alice' (#4), 'Bob' (#4)
        └─> create Contact:Contact
            with
              owner = 'Alice';
              party = 'Bob';
              address = "1-10 Bobstreet";
              telephone = "098 7654 321"

    TX #5 1970-01-01T00:00:00Z (Contact:70:3)
    #5:0
    └─> fetch #4:1 (Contact:Contact)

  Active contracts:  #4:1

  Return value: {}

There are four commits corresponding to the four ``submit`` statements in the scenario. Within each commit, we see that it's actually actions that have IDs of the form ``#commit_number:action_number``. Contract IDs are just the ID of their ``create`` action.

So commits ``#2`` and ``#4`` contain ``exercise`` actions with IDs ``#2:0`` and ``#4:0``. The ``create`` actions of the updated, ``Contact`` contracts,  ``#2:1`` and ``#4:1``, are indented and found below a line reading ``children:``, making the tree structure apparent.

The Archive choice
~~~~~~~~~~~~~~~~~~

You may have noticed that there is no archive action. That's because ``archive cid`` is just shorthand for ``exercise cid Archive``, where ``Archive`` is a choice implicitly added to every template, with the signatories as controllers.

.. _simple_iou:

A simple cash model
-------------------

With the power of choices, you can build your first interesting model: issuance of cash IOUs (I owe you). The model presented here is simpler than the one in :doc:`3_Data` as it's not concerned with the location of the physical cash, but merely with liabilities:

.. literalinclude:: daml/daml-intro-4/SimpleIou.daml
  :language: daml

The above model is fine as long as everyone trusts Dora. Dora could revoke the `SimpleIou` at any point by archiving it. However, the provenance of all transactions would be on the ledger so the owner could *prove* that Dora was dishonest and cancelled her debt.

Next up
-------

You can now store and transform data on the ledger, even giving other parties specific write access through choices.

In :doc:`5_Restrictions`, you will learn how to restrict data and transformations further. In that context, you will also learn about time on DAML ledgers, ``do`` blocks and ``<-`` notation within those.
