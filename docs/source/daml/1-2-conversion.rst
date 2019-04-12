.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Converting a project to DAML 1.2
################################

In DA SDK version 0.11, we introduce DAML version 1.2. The SDK documentation and code examples all now refer to 1.2.

This page explains how to upgrade your DAML 1.0 and 1.1 code to DAML 1.2. It doesn’t explain what the new features in 1.2 are.

On this page:

.. contents:: :local:

Introduction: the upgrade process
*********************************

This page explains how to upgrade your DAML 1.0 and 1.1 code to DAML 1.2. It's organised into groups to make it easier to follow, but you don't necessarily need to follow the steps in order (apart from the steps under "Start converting").

The list of changes here isn't comprehensive, but we've tried to cover as many things as possible, to help you upgrade.

A recommended approach is to start by converting files with no dependencies, resolving DAML Studio errors until they are all fixed. When you've fixed a file, you can then look at the files that depend on it, and so on upwards.

Start converting
****************

1. Upgrade to SDK version 0.11
==============================

If you haven't already, upgrade your project to use SDK version 0.11. For details on how to do this, see :ref:`assistant-manual-managing-releases`.

2. Change language version to 1.2
=================================

In all of your DAML files, change the version to 1.2. You need to do this for every DAML file in your codebase: you can't have a mix of versions within a project.

Templates
*********

Separate lists of observers and signatories with commas
=======================================================

Previously, lists of ``observer`` and ``signatory`` parties could be separated by semicolons or commas. Now, you can only use commas.

Before:

.. code-block:: daml

  signatory issuer; owner

After:

.. code-block:: daml

  signatory issuer, owner

Add appends (``<>``) between multi-line agreements
==================================================

In DAML 1.2, agreements are no longer a series of lines, but a one-line statement. If you have agreements that span multiple lines, you now need to concatenate them with ``<>``.

Before:

.. code-block:: daml

  agreement
    "REPO agreement was breached by the "
    toText seller.account.owner.agent


After:

.. code-block:: daml

  agreement
    "REPO agreement was breached by the " <>
    show seller.account.owner.agent

Move template member functions outside the template
===================================================

Previously, templates could have member functions declared using `def`. Now, you preface these using `let`.

Before:

.. code-block:: daml

  template Foo
    with
      p : Party
      msg : Text
    where
      signatory p

      def crFoo (msg : Msg) =
        create Foo with msg; p

After:

.. code-block:: daml

  template Foo
    with
      p : Party
      msg : Text
    where
      signatory p

      let crFoo _this msg =
        create Foo with msg; p = _this.p

Choices
*******

Change how you specify a choice's return type
=============================================

Previously, you used the keyword ``returning`` to specify a choice's return type. Now, use a ``:`` (semicolon) after the choice name.

Before:

.. code-block:: daml

  ChoiceName
    with exampleArgument : ArgType
    returning RetType

After:

.. code-block:: daml

  ChoiceName : RetType
    with exampleArgument : ArgType

Change how you introduce a choice
=================================

Previously, choice bodies were introduced with ``to`` (then, often, a ``do``). Now, use ``do``.

Before:

.. code-block:: daml

  ChoiceName
    with exampleArgument : ArgType
    returning RetType
    to do action

After:

.. code-block:: daml

  ChoiceName
    : RetType
    with exampleArgument : ArgType
    do action

Change anytime to nonconsuming
==============================

The keyword ``anytime`` has been renamed to ``nonconsuming``.

Before:

.. code-block:: daml

  controller operator can
    anytime InviteParticipant

After:

.. code-block:: daml

  controller operator can
    nonconsuming InviteParticipant

Make choice names unique
========================

Previously, different templates could duplicate choice names. Now, choice names in the same module must now be unique. For example, you can’t have two ``Accept`` choices on different templates.

We recommend adding the template name to the start of the choice.

Before:

.. code-block:: daml

  controller operator can Accept

After:

.. code-block:: daml

  controller operator can Cash_Accept

Stop re-using template argument names inside choices
====================================================

Previously, you could "shadow" a template's parameter names, using the same names for parameters to choices. In DAML 1.2, this is no longer permitted.

Consequently you can no longer reuse an argument name from template in one of its’ choices.

Before:

.. code-block:: daml

  template Foo
    with
      p : Party
      msg : Text
    where
      signatory p
      controller p can ExampleChoice
        with msg : Text

After:

.. code-block:: daml

  template Foo
    with
      p : Party
      msg : Text
    where
      signatory p
      controller p can ExampleChoice
        with otherMsg : Text

Change references to nested fields
==================================

Previously, you could do “nested field” punning. Now, you need to explicitly assign the field.

Before:

.. code-block:: daml

  create repoAgreement with
    r.buyer; r.seller; registerTime

After:

.. code-block:: daml

  create repoAgreement with
    buyer = r.buyer
    seller = r.seller; registerTime

Likely errors
-------------

If you haven't made this change yet, you might see error messages like "name not in scope", "duplicate name", "let in a do block", and indentation errors. For example:

.. code-block:: none

  /full/path/...daml:12:24: error:
      Not in scope: `x'
  /full/path/...daml:12:31: error:
      Multiple declarations of `baz'
      Declared at: ....daml:12:32
                   ....daml:12:31

Scenarios
*********

Change how you specify scenario tests
=====================================

Previously, scenarios were introduced using ``test``. You can now remove ``test``.

Before:

.. code-block:: daml

  test myTest = scenario
    action

After:

.. code-block:: daml

  myTest = scenario do
    action

Change commits and fails to submit and submitMustFail
=====================================================

``commits`` has been renamed to ``submit``, and ``fails`` has been renamed to ``submitMustFail``. The ordering of the arguments has also changed.

Before:

.. code-block:: daml

  partyA commits updateB

  partyA fails updateB

After:

.. code-block:: daml

  submit partyA do updateB
  partyA `submit` updateB -- as an alternative

  submitMustFail partyA do updateB

Replace Party literals with getParty
====================================

Party literals have been removed. Instead, use  ``getParty`` to create a value of type ``Party``.

Note that what can be in the party text is more limited now. Only alphanumeric characters, ``-``, ``_`` and spaces.

Before:

.. code-block:: daml

  let exampleParty = 'Example party'

After:

.. code-block:: daml

  exampleParty <- getParty "Example party"

Why was this change made? To ensure (using the type system) that a party could only be introduced in the context of a ``Scenario``.

For party literals in choices, see :ref:`remove-does`.

Likely errors
-------------

If you haven't made this change yet, you might see error messages like:

.. code-block:: none

   /the/full/Path/To/MyFile.daml:12:13: error:
     * Syntax error on 'Example party'
      Perhaps you intended to use TemplateHaskell or TemplateHaskellQuotes
     * In the Template Haskell quotation 'Example party'

If the party has only one character, like ``'D'``:

.. code-block:: none

  ...:12:13: error:
    * Couldn't match expected type `Party'
                  with actual type `GHC.Types.Char'
    * In the first argument of `submit', namely 'D'
      In the expression: 'D' `submit` assert $ 1 == 0
      In an equation for `wrongTest':
          wrongTest = 'D' `submit` assert $ 1 == 0

Built-in types and functions
****************************

Replace Time literals with date and time
========================================

Time literals have been removed. Instead, use the functions ``date`` and ``time``.

Before:

.. code-block:: daml

  exampleDate = 1970-01-02T00:00:00Z

After:

.. code-block:: daml

  exampleDate = date 1970 Jan 1

  exampleDateTime = time 0 0 0 exampleDate


Replace type names that have changed
====================================

- ``Integer`` is now ``Int``.
- ``List`` is now ``[Type]``; ``cons`` is now ``\::``; ``nil`` is now ``[]``.
- Empty value ``{}`` is now ``()`` .

Before:

.. code-block:: daml

  Integer

  cons 1 (cons 2 nil) : List Integer

  {}

After:

.. code-block:: daml

  Int

  (1 :: 2 :: []) : [Int]

  ()

Remove Char
===========

In DAML 1.2, the primitive type ``Char`` and all of its related functions have been removed. Use ``Text`` instead.

Before:

.. code-block:: daml

  -- Character literal for the character a
  'a'

After:

.. code-block:: daml

  -- Now, use Text instead
  "a"

Remove anonymous records
========================

You can no longer have anonymous :ref:`records <daml-ref-record-types>` - for example, returned by choices, or as choice arguments.

Instead, use a pair/tuple, or define the data type outside the template.

Before:

.. code-block:: daml

  controller owner can
    Split with splitAmount : Decimal
      returning  {
        splitCid : ContractId Iou;
        restCid : ContractId Iou
      }


After:

.. code-block:: daml

  controller owner can
    Split : (ContractId Iou, ContractId Iou)
      with splitAmount : Decimal

Or, alternatively, define the data type outside the template:

.. code-block:: daml

  data TwoCids = TwoCids with
    splitCid : ContractId Iou
    restCid : ContractId Iou

Change Tuples
=============

A Tuple no longer has fields ``fst`` and ``snd``. Instead use ``_1``, ``_2``, etc for all Tuples.

Tuple3 is now ``(,,)``.

Before:

.. code-block:: daml

  def tsum (t : Tuple2 Integer Integer)
      : Integer =
    t.fst + t.snd


After:

.. code-block:: daml

  tsum : (Int, Int) -> Int
  tsum t = t._1 + t._2

  -- or, you could do
  tsum t = fst t + snd t

Type of round function has changed
==================================

The type of the ``round`` function has changed. Replace it with ``roundBankers`` or ``roundCommercial``, depending on the rounding mode you want to use.

Before:

.. code-block:: daml

  -- using bankers’ rounding mode
  round : Integer -> Decimal -> Decimal

After:

.. code-block:: daml

  -- rounds away from zero
  round : Decimal -> Int

Replace toText with show (except on Text)
=========================================

Where you previously used ``toText``, now use ``show``.

However, note that the behaviour of ``show`` when applied to a ``Text`` value is different to the behaviour of ``toText``, because show surrounds the value with Haskell string quotes.

Before:

.. code-block:: none

  > toText "bla"
  "bla"

After:

.. code-block:: none

  > show "bla"
  ""bla""

Replace fromInteger and toInteger
=================================

Where you previously used ``fromInteger``, now use ``intToDecimal``.

Where you previously used ``toInteger``, depending on what you want to achieve, use one of:

- ``truncate`` (``truncate x`` rounds ``x`` toward zero - closest to the behaviour of ``toInteger``)
- ``round`` (round to nearest integer, where a ``.5`` is rounded away from zero)
- ``floor`` (round down to nearest integer)
- ``ceiling`` (round up to nearest integer)

.. _remove-does:

Remove does
===========

``does`` has been removed, and you should remove it alongside the ``Party`` doing the action: this is now inferred.

Before:

.. code-block:: daml

  party does action

After:

.. code-block:: daml

  action

Syntax
******

Change how you specify functions (def and fun)
==============================================

- The ``def`` keyword has been removed. You don't need to replace it with anything.
- You now separate signatures from function definitions.
- The ``fun`` keyword for lambda expressions has been remove. Instead, use ``\`` (backslash).

Before:

.. code-block:: daml

  def funcName (arg1 : Text) (arg2 : Integer) : Text = ...

  def x = 1

  map (fun x -> x * x) [1, 2, 3]

After:

.. code-block:: daml

  funcName : Text -> Int -> Text
  funcName arg1 arg2 = ...

  x = 1

  map (\x -> x * x) [1, 2, 3]


Change let
==========

``let`` in ``update`` and `do` blocks is unchanged; it still works like this:

.. code-block:: daml

  do
    let x = 1
    pure x + 40

``let`` in *expressions* has changed. It now requires the ``in`` keyword to specify wherein the bindings apply.

Before:

.. code-block:: daml

  def foo (x : Integer) : Integer =
    let y = 40
    x + y

After:

.. code-block:: daml

  foo : Int -> Int
  foo x =
    let y = 40
    in x + y

Other changes
*************

Change Standard Library imports
===============================

Standard library modules are now organized slightly differently: ``DA.<Something>`` rather than ``DA.Base.<Something>``.

Also, the standard library prelude now includes many functions that previously would need to be imported explicitly.

Before:

.. code-block:: daml

  import DA.Base.List
  import DA.Base.Map

After:

.. code-block:: daml

  import DA.List
  import DA.Map

Change Maybe
============

``Maybe`` has been renamed to ``Optional``.

Indentation rules are stricter
==============================

Previous versions of DAML were quite permissive with how much indentation you needed to add. These rules are now stricter.

Add deriving (Eq, Show) to any data type used in a template
===========================================================

Data types used in a template are now required to support classes ``Eq`` and ``Show``.

Before:

.. code-block:: daml

  data Bar = Bar
    with
      x : Integer

After:

.. code-block:: daml

  data Bar = Bar
    with
      x : Integer
    deriving (Eq, Show)

Likely errors
-------------

If you haven't made this change yet, you might see error messages like:

.. code-block:: none

  * No instance for (Eq Bar)
        arising from the second field of `Foo' (type `Bar')
      Possible fix:
        use a standalone 'deriving instance' declaration,
          so you can specify the instance context yourself
  * When deriving the instance for (Eq Foo)DAML to Core

Fix warnings on importing modules where you don't use anything exported by it
=============================================================================

Importing a module where you do not use anything exported by it now gives a warning. You can fix this warning by importing using ``import Module()``.

Before:

.. code-block:: daml

  import Foo

After:

.. code-block:: daml

  import Foo()


Update (non-DAML) application code
==================================

There is a change as a consequence of DAML 1.2 in how applications exercise choices with no parameters: they’re now exercised in a way that’s more similar to choices with parameters.

If you have an application (based on GRPC, Java binding, or something else) that exercises any choices with no parameters, you need to update that code.

In the DAML-LF produced from DAML 1.1, choices with no parameters take a Unit argument. Now, they take an argument of type data Foo = Foo, where Foo is the choice name. This is a record with zero fields.

Changes to privacy and authorization
====================================

The behavior around what parties have access to has changed in two ways:

* There used to be a situation where parties could get access to contracts that they were not stakeholders to.

  This happened for templates that have a contract ID as an argument to the template.

  Previously, when a contract was created from such a template, the contract ID passed as an argument (and so the contents of the contract) were disclosed to all parties that could see the new contract. This is no longer the case in DAML 1.2.

  The current behavior applies to all templates. Now, when a choice is exercised on a contract, that contract's ID (and so the contract's contents) are disclosed to all parties that can see the exercise.

  The reasoning behind this is to make sure that all parties seeing the exercise can recompute and confirm the result of exercising that choice.
* Previously, if a party could get hold of a contract ID, they could fetch the contract that ID related to.

  Now, fetches only succeed when at least one party that is a stakeholder of the fetched contract authorizes the fetch. Otherwise the fetch will fail.

  This means that a party can fetch a contract if and only if a stakeholder of that contract authorized the disclosure to that party.

Other errors
************

This conversion guide is not comprehensive. If you can't work out how to convert something, please :doc:`get in touch </support/support>`.
