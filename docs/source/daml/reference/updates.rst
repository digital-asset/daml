.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Reference: updates
##################

This page gives reference information on Updates:

.. contents:: :local:

For the structure around them, see :doc:`structure`.

.. _daml-ref-updates-background:

Background
**********

- An ``Update`` is ledger update. There are many different kinds of these, and they're listed below.
- They are what can go in a :ref:`choice body <daml-ref-choice-body>`.

.. _daml-ref-binding-variables:

Binding variables
*****************

.. code-block:: daml

   boundVariable <- UpdateExpression1

- One of the things you can do in a choice body is bind (assign) an Update expression to a variable. This works for any of the Updates below.

.. _daml-ref-do:

do
***

.. code-block:: daml

   do
      updateExpression1
      updateExpression2

- ``do`` can be used to group ``Update`` expressions. You can only have one update expression in a choice, so any choice beyond the very simple will use a ``do`` block.
- Anything you can put into a choice body, you can put into a ``do`` block.
- By default, ``do`` returns whatever is returned by the **last expression in the block**.

  So if you want to return something else, you'll need to use ``return`` explicitly - see :ref:`daml-ref-return` for an example.

.. _daml-ref-create:

create
******

.. literalinclude:: ../code-snippets/Reference.daml
   :language: daml
   :lines: 44
   :dedent: 8

- ``create`` keyword.
- Creates an instance of that contract on the ledger. When a contract is committed to the ledger, it is given a unique contract identifier of type ``ContractId <name of template>``.

  Creating the contract returns that ``ContractId``.
- Use ``with`` to specify the template parameters.
- Requires authorization from the signatories of the contract being created. This is given by being signatories of the contract from which the other contract is created, being the controller, or explicitly creating the contract itself.

  If the required authorization is not given, the transaction fails. For more detail on authorization, see :ref:`daml-ref-signatories`.

.. _daml-ref-exercise:

exercise
********

.. code-block:: daml

   exercise IdOfContract NameOfChoiceOnContract with choiceArgument1 = value1

- ``exercise`` keyword.
- Exercises the specified choice on the specified contract.
- Use ``with`` to specify the choice parameters.
- Requires authorization from the controller(s) of the choice. If the authorization is not given, the transaction fails.

.. _daml-ref-fetch:

fetch
*****

.. code-block:: daml

   fetchedContract <- fetch IdOfContract

- ``fetch`` keyword.
- Fetches the contract instance with that ID. Usually used with a bound variable, as in the example above.
- Often used to check the details of a contract before exercising a choice on that contract. Also used when referring to some reference data.
- ``fetch cid`` fails if ``cid`` is not the contract id of an active contract, and thus causes the entire transaction to abort. 

.. _daml-ref-abort:

abort
*****

.. code-block:: daml

   abort errorMessage

- ``abort`` function.
- Fails the transaction - nothing in it will be committed to the ledger.
- ``errorMessage`` is of type ``Text``. Use the error message to provide more context to an external system (e.g., it gets displayed in DAML Studio scenario results).
- You could use ``assert False`` as an alternative.

.. _daml-ref-assert:

assert
******

.. code-block:: daml

   assert (condition == True)

- ``assert`` keyword.
- Fails the transaction if the condition is false. So the choice can only be exercised if the boolean expression evaluates to ``True``.
- Often used to restrict the arguments that can be supplied to a contract choice.

Here's an example of using ``assert`` to prevent a choice being exercised if the ``Party`` passed as a parameter is on a blacklist:

.. literalinclude:: ../code-snippets/RestrictedPayout.daml
   :language: daml
   :lines: 35-39

.. _daml-ref-gettime:

getTime
*******

.. code-block:: daml

   currentTime <- getTime

- ``getTime`` keyword.
- Gets the ledger effective time. (You will usually want to immediately bind it to a variable in order to be able to access the value.)
- Used to restrict when a choice can be made. For example, with an ``assert`` that the time is later than a certain time.

Here's an example of a choice that uses a check on the current time:

.. literalinclude:: ../code-snippets/Snippets.daml
   :language: daml
   :lines: 42-46

.. _daml-ref-return:

return
******

.. code-block:: daml

   return ()

- ``return`` keyword.
- Used to return a value from ``do`` block that is not of type ``Update``.

Here's an example where two contracts are created in a choice and both their ids are returned as a tuple:

.. code-block:: daml

   do
     firstContract <- create SomeContractTemplate with arg1; arg2
     secondContract <- create SomeContractTemplate with arg1; arg2
     return (firstContract, secondContract)

.. _daml-ref-let-update:

let
***

See the documentation on :ref:`daml-ref-let`.

Let looks similar to binding variables, but it's very different! This code example shows how:

.. code-block:: daml

  do
    -- defines a function, createdContract, taking a single argument that when
    -- called _will_ create the new contract using argument for issuer and owner
    let createContract x = create NameOfContract with issuer = x; owner = x

    createContract party1
    createContract party2

.. _daml-ref-this:

this
****

``this`` lets you refer to the current contract from within the choice body. This refers to the contract, *not* the contract ID.

It's useful, for example, if you want to pass the current contract to a helper function outside the template.
