.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Daml Verification Tool
======================

This project performs fully automated formal verification of Daml code.
Provided with a choice of a template and a field of a (potentially different) template, the verification tool employs symbolic 
execution and an SMT solver, to verify whether the given choice always preserves
the total amount of the given field. This allows it to automatically answer
the common question "Could this smart contract model ever burn money or create
money out of thin air?"

Installation
============

- Install the `Daml development dependecies`_.

- Install the `Z3 SMT solver`_. 
  Note that on Linux, you can alternatively install Z3 using
  ``sudo apt install z3``.

.. _Daml development dependecies: https://github.com/digital-asset/daml/
.. _Z3 SMT solver: https://github.com/Z3Prover/z3

Running the Verification Tool
=============================

At the moment, the verification tool is launched from a command line interface.
Execute the tool by passing in the DAR file, the choice to be 
verified (``--choice`` / ``-c``), and the field which should be preserved (``--field`` / ``-f``):

.. code-block::
  > bazel run //compiler/daml-lf-verify:daml-lf-verify -- file.dar --choice Module:Template.Choice --field Module:Template.Field

Examples
========

Example 1: Transferring value
-----------------------------

As a first example, consider a simple ``Account`` template, with a single choice
``Account_Transfer`` for transfering from one account to another. Note that if
someone tries to transfer more than they own, the choice should just do nothing.

.. code-block:: daml
  module Demo where
  template Account
    with
      amount : Decimal
      owner : Party
    where
      ensure amount > 0.0
  
      signatory owner
  
      -- | Transfer some amount to the receiver, or do nothing if this would make the
      -- remaining amount in the account negative.
      nonconsuming choice Account_Transfer : (ContractId Account, ContractId Account)
        with
          transferAmount: Decimal
          receiverCid: ContractId Account
        controller owner
        do
          if amount >= transferAmount
          then do
            -- The account has sufficient funds.
            receiver <- fetch receiverCid
            newSelf <- create this with amount = amount - transferAmount
            newReceiver <- create receiver with
              amount = receiver.amount + transferAmount
            archive receiverCid
            return (newSelf, newReceiver)
          else do
            -- The account does not have sufficient funds, and the transfer is
            -- cancelled.
            return (self, receiverCid)

It is clear that making a transfer between two accounts, should always preserve
the total amount of funds. However, running the verification tool produces the 
following output:

.. code-block::
  > bazel run //compiler/daml-lf-verify:daml-lf-verify -- demo.dar --choice Demo:Account.Account_Transfer --field Demo:Account.amount
  
  ...
  
  ==========
  
  Main flow: 
  
  Create:  [ if  this_6.amount >= arg_5.transferAmount  then  this_6.amount  -  arg_5.transferAmount  else  0 % 1
  , if  this_6.amount >= arg_5.transferAmount  then  receiver_9.amount  +  arg_5.transferAmount  else  0 % 1 ] 
  Archive:  [ if  this_6.amount >= arg_5.transferAmount  then  receiver_9.amount  else  0 % 1 ] 
  Assert: (+ receiver_9.amount arg_5.transferAmount) = this_12.amount
  Assert: (- this_6.amount arg_5.transferAmount) = this_10.amount
  Assert: receiver_9.amount > 0.0
  Assert: (- this_6.amount arg_5.transferAmount) = this_10.amount
  Assert: this_6.amount > 0.0
  Assert: this_6.amount > 0.0
  Assert: receiver_9.amount > 0.0
  ~~~~~~~~~~
  
  Fail. The choice Account_Transfer does not preserve the field amount. Counter example:
  this_10.amount = (/ 1.0 2.0)
  this_12.amount = 0.0
  receiver_9.amount = (/ 1.0 4.0)
  arg_5.transferAmount = (-
     (/ 1.0 4.0))
  this_6.amount = (/ 1.0 4.0)
  
  ==========
  
  Done.

The ``Main flow`` describes all updates performed when exercising the 
``Account_Transfer`` choice, without making any additional assumptions on the 
inputs. It enumerates all create and archive updates, along with a number of
additional assertions (e.g. arising from the ``ensure`` statement in ``Account``).

The SMT solver figured out that the choice does not actually preserve the total
amount, and provides a handy counter example. In fact, it turns out that we're
creating more money than archiving. A closer look at the create and archive
updates clearly shows that we're never archiving ``this_6.amount``. And indeed,
we forgot to include ``archive self`` in the example. After
adding this line to the definition of ``Account_Transfer``, the new output is as 
follows:

.. code-block::
  > bazel run //compiler/daml-lf-verify:daml-lf-verify -- demo.dar --choice Demo:Account.Account_Transfer --field Demo:Account.amount
  
  ...
  
  ==========
  
  Main flow: 
  
  Create:  [ if  this_6.amount >= arg_5.transferAmount  then  this_6.amount  -  arg_5.transferAmount  else  0 % 1
  , if  this_6.amount >= arg_5.transferAmount  then  receiver_9.amount  +  arg_5.transferAmount  else  0 % 1 ] 
  Archive:  [ if  this_6.amount >= arg_5.transferAmount  then  this_6.amount  else  0 % 1
  , if  this_6.amount >= arg_5.transferAmount  then  receiver_9.amount  else  0 % 1 ] 
  Assert: (+ receiver_9.amount arg_5.transferAmount) = this_12.amount
  Assert: (- this_6.amount arg_5.transferAmount) = this_10.amount
  Assert: receiver_9.amount > 0.0
  Assert: (- this_6.amount arg_5.transferAmount) = this_10.amount
  Assert: this_6.amount > 0.0
  Assert: this_6.amount > 0.0
  Assert: receiver_9.amount > 0.0
  ~~~~~~~~~~
  
  Success! The choice Account_Transfer preserves the field amount.
  
  ==========
  
  Done.

Now the verification tool can prove that ``Account_Transfer`` does in fact
preserve the ``amount`` field.

Example 2: Recursion
--------------------

For a second example, consider the ``Account_Divide`` choice, which recursively
donates 1.0 amount from the donor to the receiver account, until the receiver
account has at least as much funds as the donor.

.. code-block:: daml
        -- | Iteratively transfer 1.0 amount to the receiver, until it has at
        -- least as much funds as the donor.
        nonconsuming Account_Divide : (ContractId Account, ContractId Account)
          with
            receiverCid: ContractId Account
          do
            receiverAccount <- fetch receiverCid
            if amount <= receiverAccount.amount
            -- The receiver has at least as much funds as the donor.
            then return (self, receiverCid)
            -- The receiver does not yet have enough funds. Make a new transaction.
            else do
              (newSelf, newReceiver) <- exercise self Account_Transfer with 
                transferAmount = 1.0, receiverCid = receiverCid
              exercise newSelf Account_Divide with receiverCid = newReceiver

We can again use the formal verification tool to ensure that ``Account_Divide``
always preserves the field ``amount``.

.. code-block::
  > bazel run //compiler/daml-lf-verify:daml-lf-verify -- demo.dar --choice Demo:Account.Account_Divide --field Demo:Account.amount
  
  ...
  
  ==========
  
  Main flow: 
  
  Create:  [ if  not  this_18.amount <= receiverAccount_21.amount  and  this_18.amount >= 1 % 1  then  this_18.amount  -  1 % 1  else  0 % 1
  , if  not  this_18.amount <= receiverAccount_21.amount  and  this_18.amount >= 1 % 1  then  receiver_9.amount  +  1 % 1  else  0 % 1 ] 
  Archive:  [ if  not  this_18.amount <= receiverAccount_21.amount  and  this_18.amount >= 1 % 1  then  this_18.amount  else  0 % 1
  , if  not  this_18.amount <= receiverAccount_21.amount  and  this_18.amount >= 1 % 1  then  receiver_9.amount  else  0 % 1 ] 
  Assert: this_18.amount > 0.0
  Assert: receiverAccount_21.amount > 0.0
  Assert: (+ receiver_9.amount arg_5.transferAmount) = this_12.amount
  Assert: (- this_6.amount arg_5.transferAmount) = this_10.amount
  Assert: receiver_9.amount > 0.0
  Assert: (- this_6.amount arg_5.transferAmount) = this_10.amount
  Assert: this_6.amount > 0.0
  Assert: this_6.amount > 0.0
  Assert: receiver_9.amount > 0.0
  ~~~~~~~~~~
  
  Success! The choice Account_Divide preserves the field amount.
  
  ==========
  
  Recursion cycle: 
  
  Create:  [ if  not  this_18.amount <= receiverAccount_21.amount  and  this_18.amount >= 1 % 1  then  this_18.amount  -  1 % 1  else  0 % 1
  , if  not  this_18.amount <= receiverAccount_21.amount  and  this_18.amount >= 1 % 1  then  receiver_9.amount  +  1 % 1  else  0 % 1 ] 
  Archive:  [ if  not  this_18.amount <= receiverAccount_21.amount  and  this_18.amount >= 1 % 1  then  this_18.amount  else  0 % 1
  , if  not  this_18.amount <= receiverAccount_21.amount  and  this_18.amount >= 1 % 1  then  receiver_9.amount  else  0 % 1 ] 
  Assert: this_18.amount > 0.0
  Assert: receiverAccount_21.amount > 0.0
  Assert: (+ receiver_9.amount arg_5.transferAmount) = this_12.amount
  Assert: (- this_6.amount arg_5.transferAmount) = this_10.amount
  Assert: receiver_9.amount > 0.0
  Assert: (- this_6.amount arg_5.transferAmount) = this_10.amount
  Assert: this_6.amount > 0.0
  Assert: this_6.amount > 0.0
  Assert: receiver_9.amount > 0.0
  ~~~~~~~~~~
  
  Success! The choice Account_Divide preserves the field amount.
  
  ==========
  
  Done.

Note that besides the ``Main flow`` verification, the tool also isolates any
(mutual) recursion cycles within the choice. In this scenario, ``Account_Divide``
has one recursion cycle, shown under ``Recursion cycle``. In order for a field
to be preserved, the choice has to always terminate, and every cycle has to
preserve the given field.

Testing Framework
=================

A testing framework is provided for working on the verification tool. Run the
tests as follows:

.. code-block::
  > bazel run //compiler/daml-lf-verify:verify-tests
  ...
  All 23 tests passed (9.44s)
