.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Data Transformations and Choices
================================

In the example in :ref:`contract_keys` the accountant party wanted to change some data on a contract. They did so by archiving and re-creating the contract with the updated data. That works because the accountant is the sole signatory on the `Account` contract defined there. But what if the accountant wanted to allow the bank to change their own telephone number, or what if the owner of a CashBalance should be able to transfer ownership to someone else?

In this section you will learn about how to define simple data transformations using *choices* and how to delegate the right to *exercise* these choices to other parties.

Choices as Methods
------------------

*Choices* are permissioned functions that result in an `Update`. Using choices, authority can be passed around allowing the construction of complex transactions. The simplest choices can be thought of simply as methods on objects, with templates as classes and contracts as objects. Take as an example a ``Contact`` contract on which the contact owner wants to be able to change the telephone number, just like on the ``Account`` in :ref:`contract_keys`. Rather than requiring them to manually look up the contract, archive the old one and create a new one, you can provide them a convenience method on ``Contact``:

.. literalinclude:: daml/4_Transformations/Contact.daml
  :language: daml
  :start-after: -- CHOICE_BEGIN
  :end-before: -- CHOICE_END

There's a lot to unpack in the above.

- The first line, ``controller owner can`` says that the following choices are *controlled* by ``owner``, meaning ``owner`` is the party that may *exercise* them. The line starts a new block in which multiple choices can be defined.
- ``UpdateTelephone`` is the name of a choice. It starts a new block in which that choice is defined.
- ``: ContractId Contact`` is the return type of the choice. This choice archives the current ``Contact`` and creates a new one. It returns a reference to the new contract in the form of a ``ContractId Contact``
- The following ``with`` block is that of a record. Just like with templates, in the background, a new record type is declared: ``data UpdateTelephone = UpdateTelephone with``
- The `do` starts a block defining the action the choice should perform when exercised. In this case a new ``Contact`` is created.
- The new ``Contact`` is created using ``this with``. ``this`` is a special value available within the ``where`` block of templates and takes the value of the current contract's arguments.

There is nothing here indicating that the current ``Contact`` should be archived. That's because choices are *consuming* by default. That means when the above choice is exercised on a contract, that contract is archived.

If you paid a lot of attention in :doc:`3_Data`, you may have noticed that the ``create`` statement returns an ``Update (ContractId Contact)``, not a ``ContractId Contact``. As a ``do`` block always returns the value of the last statement within it, the whole ``do`` block returns an ``Update``, but the return type on the choice is just a ``ContractId Contact``. This is a conveneience. Choices *always* return an ``Update`` so for readability it's omitted on the type declaration of a choice.

Now to exercise the new choice in a scenario:

.. literalinclude:: daml/4_Transformations/Contact.daml
  :language: daml
  :start-after: -- CHOICE_TEST_BEGIN
  :end-before: -- CHOICE_TEST_END

Choices are exercised using the ``exercise`` function, which takes a ``ContractId a``, and a value of type ``c``, where ``c`` is a choice on template ``a``. Since ``c`` is just a record, you can also just fill in the choice parameters using the ``with`` syntax you are already familiar with. ``exercise`` returns an ``Update r`` where ``r`` is the return type specified on the choice, allowing the new ``ContractId Contact`` to be stored in the variable ``new_acontactCid``.

Choices as Delegation
---------------------

Up to this point all the contracts only involved one party. ``party`` may have been stored as ``Party`` field in the above, which suggests they are actors on the ledger, but they couldn't see the contracts, nor change them in any way. It would be reasonable for the party for which a ``Contact`` is stored to be able to update their own address and telephone number. The below demonstrates this using an ``UpdateAddress`` choice and corresponding extension of the scenario:

.. literalinclude:: daml/4_Transformations/Contact.daml
  :language: daml
  :start-after: -- DELEGATION_BEGIN
  :end-before: -- DELEGATION_END

.. literalinclude:: daml/4_Transformations/Contact.daml
  :language: daml
  :start-after: -- DELEGATION_TEST_BEGIN
  :end-before: -- DELEGATION_TEST_END

If you open the scenario view in the IDE, you will notice that Bob sees the ``Contact``. Controllers specified via ``controller c can`` syntax become *observers* of the contract. More on *observers* later, but in short, they get to see any changes to the contract.

Choices in the Ledger Model
---------------------------

It's time for a bit more theory. in :doc:`1_Token` you learned about the high-level structure of a DAML ledger. With choices and the `exercise` function, you have the next important ingredient. A *transaction* is a list of *actions*, and there are just three kinds of action: ``create``, ``exercise`` and ``fetch``. All actions are performed on a contract.

- A ``create`` action contains the contract arguments and changes the contract's status from *inexistent* to *active*.
- A ``fetch`` action  checks the existence and activeness of a contract
- An ``exercise`` action contains the choice arguments and a transaction called the *consequences*. Exercises come in two *kinds* called ``consuming`` and ``nonconsuming``. ``consuming`` is the default kind and changes the contract's status from *active* to *archived*.

The consequences of exercise nodes turn each transaction into an ordered tree of (sub-) transactions, or, equivalently, a forest of actions. Actions are in one-to-one correspondence with proper sub-transactions.

Each contract has an implicit choice ``Archive`` with the signatories as controllers and ``archive cid`` is just shorthand for ``exercise cid Archive``.

With that new background, it's time to take a look at the transaction view of the above scenario:

.. code-block:: none

  Transactions:
    TX #0 1970-01-01T00:00:00Z (Contact:40:17)
    #0:0
    │   consumed by: #2:1
    │   referenced by #2:0, #2:1
    │   known to (since): 'Alice' (#0), 'Bob' (#0)
    └─> create Contact:Contact
        with
          owner = 'Alice'; party = 'Bob'; address = "1 Bobstreet"; telephone = "012 345 6789"

    TX #1 1970-01-01T00:00:00Z
      mustFailAt 'Bob' (Contact:49:3)

    TX #2 1970-01-01T00:00:00Z (Contact:53:22)
    #2:0
    └─> fetch #0:0 (Contact:Contact)

    #2:1
    │   known to (since): 'Alice' (#2), 'Bob' (#2)
    └─> 'Alice' exercises UpdateTelephone on #0:0 (Contact:Contact)
                with
                  newTelephone = "098 7654 321"
        children:
        #2:2
        │   consumed by: #4:1
        │   referenced by #3:0, #4:0, #4:1
        │   known to (since): 'Alice' (#2), 'Bob' (#2)
        └─> create Contact:Contact
            with
              owner = 'Alice'; party = 'Bob'; address = "1 Bobstreet"; telephone = "098 7654 321"

    TX #3 1970-01-01T00:00:00Z (Contact:57:3)
    #3:0
    └─> fetch #2:2 (Contact:Contact)

    TX #4 1970-01-01T00:00:00Z (Contact:63:22)
    #4:0
    └─> fetch #2:2 (Contact:Contact)

    #4:1
    │   known to (since): 'Alice' (#4), 'Bob' (#4)
    └─> 'Bob' exercises UpdateAddress on #2:2 (Contact:Contact)
              with
                newAddress = "1-10 Bobstreet"
        children:
        #4:2
        │   referenced by #5:0
        │   known to (since): 'Alice' (#4), 'Bob' (#4)
        └─> create Contact:Contact
            with
              owner = 'Alice';
              party = 'Bob';
              address = "1-10 Bobstreet";
              telephone = "098 7654 321"

    TX #5 1970-01-01T00:00:00Z (Contact:67:3)
    #5:0
    └─> fetch #4:2 (Contact:Contact)

  Active contracts:  #4:2

  Return value: {}

There are four commits corresponding to the four ``submit`` statements in the scenario. Within each commit, we see that it's actually actions that have ids of the form ``#commit_number:action_number``. Contract ids are just the id of their ``create`` action. Commits ``#2`` and ``#4`` contain ``exercise`` actions with ids ``#2:1`` and ``#4:1``. The ``create`` actions of the updated, ``Contact`` contracts,  ``#2:2`` and ``#4:2``, are indented and found below a line reading ``children:``, making the tree structure apparent.

There are a few extra ``fetch`` nodes scattered around. These are inserted as activeness checks. As they have no effect, it's safe not to worry about them.

.. _simple_iou:

A Simple Cash Model
-------------------

With the power of choices, you can build you first interesting model: Issuance of cash Ious (I owe you). The model presented here is simpler than the one in :doc:`3_Data` as it's not concerned with the location of the physical cash, but merely with liabilities.

.. literalinclude:: daml/4_Transformations/SimpleIou.daml
  :language: daml

The above model is fine as long as everyone trusts Dora. Dora could revoke the `SimpleIou` at any point by archiving it. However, the provenance of all transactions would be on the ledger so the owner could *prove* that Dora was dishonest and cancelled her debt.
