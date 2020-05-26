.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Troubleshooting
###############

.. contents::
    :local:

.. _faqs-multi-obligables:

Error: "<X> is not authorized to commit an update"
***************************************************

This error occurs when there are multiple obligables on a contract.

A cornerstone of DAML is that you cannot create a contract that will force some other party (or parties) into an obligation. This error means that a party is trying to do something that would force another parties into an agreement without their consent.

To solve this, make sure each party is entering into the contract freely by exercising a choice. A good way of ensuring this is the "initial and accept" pattern: see the DAML patterns for more details.

.. _faqs-serial-arg:

Error "Argument is not of serializable type"
*********************************************

This error occurs when you're using a function as a parameter to a template. For example, here is a contract that creates a ``Payout`` controller by a receiver's supervisor:

.. literalinclude:: code-snippets/troubleshooting/ObligableErrors.daml
  :language: daml
  :lines: 125-135

Hovering over the compilation error displays:

.. code-block:: none

  [Type checker] Argument expands to non-serializable type Party -> Party.

Modelling questions
*******************

.. _faqs-proposal:

How to model an agreement with another party
============================================

To enter into an agreement, create a contract instance from a template that has explicit ``signatory`` and ``agreement`` statements.

You'll need to use a series of contracts that give each party the chance to consent, via a contract choice.

Because of the rules that DAML enforces, it is not possible for a single party to create an instance of a multi-party agreement. This is because such a creation would force the other parties into that agreement, without giving them a choice to enter it or not.

.. _faqs-rights:

How to model rights
===================

Use a contract choice to model a right. A party exercises that right by exercising the choice.

.. _faqs-voiding-contract:

How to void a contract
======================

To allow voiding a contract, provide a choice that does not create any new contracts. DAML contracts are archived (but not deleted) when a consuming choice is made - so exercising the choice effectively voids the contract.

However, you should bear in mind who is allowed to void a contract, especially without the re-sought consent of the other signatories.

.. _faqs-party-delegation:

How to represent off-ledger parties
===================================

You'd need to do this if you can't set up all parties as ledger participants, because the DAML ``Party`` type gets associated with a cryptographic key and can so only be used with parties that have been set up accordingly.

To model off-ledger parties in DAML, they must be represented on-ledger by a participant who can sign on their behalf. You could represent them with an ordinary ``Text`` argument.

This isn't very private, so you could use a numeric ID/an accountId to identify the off-ledger client.

.. _faqs-timed-choice:

How to limit a choice by time
=============================

Some rights have a time limit: either a time by which it must be exercised or a time before which it cannot be exercised.

You can use ``getTime`` to get the current time, and compare your desired time to it. Use ``assert`` to abort the choice if your time condition is not met.

.. _faqs-mandatory-action:

How to model a mandatory action
===============================

If you want to ensure that a party takes some action within a given time period. Might want to incur a penalty if they don't - because that would breach the contract.

For example: an Invoice that must be paid by a certain date, with a penalty (could be something like an added interest charge or a penalty fee). To do this, you could have a time-limited Penalty choice that can only be exercised *after* the time period has expired.

However, note that the penalty action can only ever create another contract on the ledger, which represents an agreement between all parties that the initial contract has been breached. Ultimately, the recourse for any breach is legal action of some kind. What DAML provides is provable violation of the agreement.

.. _faqs-optional:

When to use Optional
====================

The ``Optional`` type, from the standard library, to indicate that a value is optional, i.e, that in some cases it may be missing.

In functional languages, ``Optional`` is a better way of indicating a missing value than using the more familiar value "NULL", present in imperative languages like Java.

To use ``Optional``, include ``Optional.daml`` from the standard library:

.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :lines: 7

Then, you can create ``Optional`` values like this:

.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :lines: 12,14
  :dedent: 6

You can test for existence in various ways:

.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :lines: 16-19,21-24
  :dedent: 6

If you need to extract the value, use the ``optional`` function.

It returns a value of a defined type, and takes a ``Optional`` value and a function that can transform the value contained in a ``Some`` value of the ``Optional`` to that type. If it is missing ``optional`` also takes a value of the return type (the default value), which will be returned if the ``Optional`` value is ``None``

.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :lines: 27-28
  :dedent: 2

If ``optionalValue`` is ``Some 5``, the value of ``t`` would be ``"The number is 5"``. If it was ``None``, ``t`` would be ``"No number"``. Note that with ``optional``, it is possible to return a different type from that contained in the ``Optional`` value. This makes the ``Optional`` type very flexible.

There are many other functions in "Optional.daml" that let you perform familiar functional operations on structures that contain ``Optional`` values – such as ``map``, ``filter``, etc. on ``Lists`` of ``Optional`` values.

Testing questions
*****************

.. _faqs-test-visibility:

How to test that a contract is visible to a party
=================================================

Use a ``submit`` block and a ``fetch`` operation. The ``submit`` block tests that the contract (as a ``ContractId``) is visible to that party, and the ``fetch`` tests that it is valid, i.e., that the contract does exist.

For example, if we wanted to test for the existence and visibility of an ``Invoice``, visible to 'Alice', whose ContractId is bound to `invoiceCid`, we could say:

.. literalinclude:: code-snippets/troubleshooting/Check.daml
  :language: daml
  :lines: 23-24
  :dedent: 4

You could also check (in the ``submit`` block) that the contract has some expected values:

.. literalinclude:: code-snippets/troubleshooting/Check.daml
  :language: daml
  :lines: 25-30
  :dedent: 6

using an equality test and an ``assert``:

.. literalinclude:: code-snippets/troubleshooting/Check.daml
  :language: daml
  :lines: 23-30
  :dedent: 4

.. _faqs-must-fail:

How to test that an update action cannot be committed
======================================================

Use the ``submitMustFail`` function. This is similar in form to the ``submit`` function, but is an assertion that an update will fail if attempted by some Party.
