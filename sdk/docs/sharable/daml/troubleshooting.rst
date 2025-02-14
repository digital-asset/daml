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

Error: "Recursion limit overflow in module"
*******************************************

The error usually occurs when uploading a DAR to a ledger or using a script
via the sandbox. It can manifest on upload as:

.. code:: text

  upload-dar did not succeed: DAR_PARSE_ERROR(8,b5935497): Failed to parse the dar file content.

or in your logs as:

.. code:: text

  Recursion limit overflow in module '<pkgid>:<modulename>'

This error is usually caused by having an expression in the DAR whose
serialized representation exceeds a depth of 1000 layers. This can be caused by
long Daml scripts, since every use of a function call or ``<-`` to bind a
variable in a ``do`` block can incur several layers of recursion in the ``do``
block's serialized representation. Large datatypes
with more than 160 fields with ``deriving`` clauses can also cause this.

Solving script recursion limits
===============================

Normally, one call inside ``do`` introduces 4 layers of recursion, meaning
about 250 binds in a script can cause an overflow. However, other expressions in a
do block, such as let binds, also introduce a layer of recursion, so
functions with fewer binds can also trigger the limit.

Possible workarounds include splitting a large script into multiple
scripts or separating logic in the script out into helper functions. For
example, assume you have written the following long script:

.. code:: daml

  data State = State { partA : Text, partB : Text, partC : Text }

  -- MyTemplate defines three choices that update parts A, B, and C of the State
  myScript : Party -> ContractId MyTemplate -> State -> Script State
  myScript party cid state0 = do
    newPartA <- party `submit` exerciseCmd cid (UpdateStatePartA state0)
    newPartB <- party `submit` exerciseCmd cid (UpdateStatePartB state0)
    newPartC <- party `submit` exerciseCmd cid (UpdateStatePartC state0)
    let state1 = State newPartA newPartB newPartC

    newPartA <- party `submit` exerciseCmd cid (UpdateStatePartA state1)
    newPartB <- party `submit` exerciseCmd cid (UpdateStatePartB state1)
    newPartC <- party `submit` exerciseCmd cid (UpdateStatePartC state1)
    let state2 = State newPartA newPartB newPartC

    ...
    newPartA <- party `submit` exerciseCmd cid (UpdateStatePartA state99)
    newPartB <- party `submit` exerciseCmd cid (UpdateStatePartB state99)
    newPartC <- party `submit` exerciseCmd cid (UpdateStatePartC state99)
    let state100 = State newPartA newPartB newPartC

    pure state100

This script has 300 binds, well exceeding the tentative limit of 250
binds. We can refactor this script to instead define and use a helper
``updateStateOnce``, which runs all three choices together.

**Note:** In many cases, the compiler optimizes your script to produce an
expression that does not break the recursion limit despite having many binds. In
this example, we have given an intentionally convoluted example that is
difficult for the compiler to optimize away.

By using this helper, each block of three exercises is reduced to a single bind,
such that ``myScript`` now has only 100 binds, well below the recursion limit.

.. code:: daml

  data State = State { partA : Text, partB : Text, partC : Text }
    deriving (Show, Eq)

  helper : Party -> ContractId MyTemplate -> State -> Script State
  helper party cid state = do
    newPartA <- party `submit` exerciseCmd cid (UpdateStatePartA state.partA)
    newPartB <- party `submit` exerciseCmd cid (UpdateStatePartB state.partB)
    newPartC <- party `submit` exerciseCmd cid (UpdateStatePartC state.partC)
    pure (State newPartA newPartB newPartC)

  myScript : Party -> ContractId MyTemplate -> State -> Script State
  myScript party cid state0 = do
    state1 <- helper party cid state0
    state2 <- helper party cid state1
    ...
    state100 <- helper party cid state99

    pure state100

In general, it is a good idea to keep your scripts small, by maximizing code
reuse and splitting logic into maintainable chunks.

Solving datatype recursion limits
=================================

Large datatypes with ``deriving`` clauses can also cause overflow errors.
Consider the following case of a datatype with 300 fields and a ``deriving Show``
instance:

.. code:: daml

  data MyData = MyData
    { field1A : Text
    , field1B : Text
    , field1C : Text
    , field2A : Text
    , field2B : Text
    , field2C : Text
    ...
    , field100A : Text
    , field100B : Text
    , field100C : Text
    }
    deriving Show

In this case, the ``deriving Show`` clause means that instance of ``Show MyData``
is automatically derived to be something like the following:

.. code:: daml

  -- Something similar to this code is implicitly autogenerated by the `deriving Show` clause
  instance Show MyData where
    show MyData {..} =
      show field1A <> (show field1B <> (show field1C <> (
        (show field2A <> (show field2B <> (show field2C <> (
          ...
          (show field100A <> (show field100B <> (show field100C)))
        ))))
      )))

You can see that the implicit, autogenerated definition of ``show`` has an
expression that becomes more and more deeply nested as you go along. Once the
number of fields exceeds about 160, this expression has the potential to reach
the depth necessary to cause an overflow in its serialized representation in the
DAR.

Similarly to Daml Scripts, the recommended workaround for this issue is to split
your datatype into many parts. For example, we could create an additional
datatype ``Helper`` which contains the elements ``a``, ``b``, and ``c``, and use
that within ``MyData``:

.. code:: daml

  data Helper = Helper
    { a : Text
    , b : Text
    , c : Text
    }
    deriving Show

  data MyData = MyData
    { field1 : Helper
    , field2 : Helper
    ...
    , field100 : Helper
    }
    deriving Show

The generated code for ``MyData`` now has only 100 fields to traverse and
nest. Similarly
to scripts, it is a good idea to keep your datatypes small, by maximizing code
reuse and splitting logic into maintainable chunks.


Modeling Questions
******************

.. _faqs-proposal:

How To Model an Agreement With Another Party
============================================

To enter into an agreement, create a contract from a template that has an explicit ``signatory`` statement.

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
