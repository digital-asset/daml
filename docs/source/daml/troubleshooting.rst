.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Troubleshooting
###############

.. _faqs-multi-obligables:

Error: "<X> is not authorized to commit an update"
**************************************************

This error occurs when there are multiple obligables on a contract.

A cornerstone of Daml is that you cannot create a contract that will force some other party (or parties) into an obligation. This error means that a party is trying to do something that would force another parties into an agreement without their consent.

To solve this, make sure each party is entering into the contract freely by exercising a choice. A good way of ensuring this is the "initial and accept" pattern: see the Daml patterns for more details.

.. _faqs-serial-arg:

Error: "Argument is not of serializable type"
*********************************************

This error occurs when you're using a function as a parameter to a template. For example, here is a contract that creates a ``Payout`` controller by a receiver's supervisor:

.. literalinclude:: code-snippets/troubleshooting/ObligableErrors.daml
  :language: daml
  :start-after: -- BEGIN_NOT_SERIALIZABLE_TEMPLATE
  :end-before: -- END_NOT_SERIALIZABLE_TEMPLATE

Hovering over the compilation error displays:

.. code-block:: none

  [Type checker] Argument expands to non-serializable type Party -> Party.

Modeling Questions
******************

.. _faqs-proposal:

How To Model an Agreement With Another Party
============================================

To enter into an agreement, create a contract from a template that has explicit ``signatory`` and ``agreement`` statements.

You'll need to use a series of contracts that give each party the chance to consent, via a contract choice.

Because of the rules that Daml enforces, it is not possible for a single party to create an instance of a multi-party agreement. This is because such a creation would force the other parties into that agreement, without giving them a choice to enter it or not.

.. _faqs-rights:

How To Model Rights
===================

Use a contract choice to model a right. A party exercises that right by exercising the choice.

.. _faqs-voiding-contract:

How To Void a Contract
======================

To allow voiding a contract, provide a choice that does not create any new contracts. Daml contracts are archived (but not deleted) when a consuming choice is made - so exercising the choice effectively voids the contract.

However, you should bear in mind who is allowed to void a contract, especially without the re-sought consent of the other signatories.

.. _faqs-party-delegation:

How To Represent Off-ledger Parties
===================================

You'd need to do this if you can't set up all parties as ledger participants, because the Daml ``Party`` type gets associated with a cryptographic key and can so only be used with parties that have been set up accordingly.

To model off-ledger parties in Daml, they must be represented on-ledger by a participant who can sign on their behalf. You could represent them with an ordinary ``Text`` argument.

This isn't very private, so you could use a numeric ID/an accountId to identify the off-ledger client.

.. _faqs-timed-choice:

How To Limit a Choice by Time
=============================

Some rights have a time limit: either a time by which it must be exercised or a time before which it cannot be exercised.

You can use ``getTime`` to get the current time, and compare your desired time to it. Use ``assert`` to abort the choice if your time condition is not met.

.. _faqs-mandatory-action:

How To Model a Mandatory Action
===============================

If you want to ensure that a party takes some action within a given time period. Might want to incur a penalty if they don't - because that would breach the contract.

For example: an Invoice that must be paid by a certain date, with a penalty (could be something like an added interest charge or a penalty fee). To do this, you could have a time-limited Penalty choice that can only be exercised *after* the time period has expired.

However, note that the penalty action can only ever create another contract on the ledger, which represents an agreement between all parties that the initial contract has been breached. Ultimately, the recourse for any breach is legal action of some kind. What Daml provides is provable violation of the agreement.

.. _faqs-optional:

When to Use Optional
====================

The ``Optional`` type, from the standard library, to indicate that a value is optional, i.e, that in some cases it may be missing.

In functional languages, ``Optional`` is a better way of indicating a missing value than using the more familiar value "NULL", present in imperative languages like Java.

To use ``Optional``, include ``Optional.daml`` from the standard library:

.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :start-after: -- start snippet: import Optional
  :end-before: -- end snippet: import Optional

Then, you can create ``Optional`` values like this:

.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :start-after: -- start snippet: create some
  :end-before: -- end snippet: create some
  :dedent: 6

.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :start-after: -- start snippet: create none
  :end-before: -- end snippet: create none
  :dedent: 6

You can test for existence in various ways:


.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :start-after: -- start snippet: test some
  :end-before: -- end snippet: test some
  :dedent: 6

.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :start-after: -- start snippet: test none
  :end-before: -- end snippet: test none
  :dedent: 6

If you need to extract the value, use the ``optional`` function.

It returns a value of a defined type, and takes a ``Optional`` value and a function that can transform the value contained in a ``Some`` value of the ``Optional`` to that type. If it is missing ``optional`` also takes a value of the return type (the default value), which will be returned if the ``Optional`` value is ``None``

.. literalinclude:: code-snippets/troubleshooting/OptionalDemo.daml
  :language: daml
  :start-after: -- start snippet: optional
  :end-before: -- end snippet: optional
  :dedent: 2

If ``optionalValue`` is ``Some 5``, the value of ``t`` would be ``"The number is 5"``. If it was ``None``, ``t`` would be ``"No number"``. Note that with ``optional``, it is possible to return a different type from that contained in the ``Optional`` value. This makes the ``Optional`` type very flexible.

There are many other functions in "Optional.daml" that let you perform familiar functional operations on structures that contain ``Optional`` values – such as ``map``, ``filter``, etc. on ``Lists`` of ``Optional`` values.

Testing Questions
*****************

.. _faqs-test-visibility:

How To Test That a Contract Is Visible to a Party
=================================================

Use ``queryContractId``: its first argument is a party, and the second is a ``ContractId``. If the contract corresponding to that ``ContractId`` exists and is visible to the party, the result will be wrapped in ``Some``, otherwise the result will be ``None``.

Use a ``submit`` block and a ``fetch`` operation. The ``submit`` block tests that the contract (as a ``ContractId``) is visible to that party, and the ``fetch`` tests that it is valid, i.e., that the contract does exist.

For example, if we wanted to test for the existence and visibility of an ``Invoice``, visible to 'Alice', whose ContractId is bound to `invoiceCid`, we could say:

.. literalinclude:: code-snippets/troubleshooting/Check.daml
  :language: daml
  :start-after: -- start snippet: query contract id
  :end-before: -- end snippet: query contract id
  :dedent: 4

Note that we pattern match on the ``Some`` constructor. If the contract doesn't exist or is not visible to 'Alice', the test will fail with a pattern match error.

Now that the contract is bound to a variable, we can check whether it has some expected values:

.. literalinclude:: code-snippets/troubleshooting/Check.daml
  :language: daml
  :start-after: -- start snippet: check contract contents
  :end-before: -- end snippet: check contract contents
  :dedent: 4

.. _faqs-must-fail:

How To Test That an Update Action Cannot Be Committed
======================================================

Use the ``submitMustFail`` function. This is similar in form to the ``submit`` function, but is an assertion that an update will fail if attempted by some Party.
