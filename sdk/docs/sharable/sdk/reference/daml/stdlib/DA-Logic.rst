.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-logic-59184:

DA.Logic
========

Logic \- Propositional calculus\.

Data Types
----------

.. _type-da-logic-types-formula-34794:

**data** `Formula <type-da-logic-types-formula-34794_>`_ t

  A ``Formula t`` is a formula in propositional calculus with
  propositions of type t\.

  .. _constr-da-logic-types-proposition-6173:

  `Proposition <constr-da-logic-types-proposition-6173_>`_ t

    ``Proposition p`` is the formula p

  .. _constr-da-logic-types-negation-48969:

  `Negation <constr-da-logic-types-negation-48969_>`_ (`Formula <type-da-logic-types-formula-34794_>`_ t)

    For a formula f, ``Negation f`` is ¬f

  .. _constr-da-logic-types-conjunction-51637:

  `Conjunction <constr-da-logic-types-conjunction-51637_>`_ \[`Formula <type-da-logic-types-formula-34794_>`_ t\]

    For formulas f1, \.\.\., fn, ``Conjunction [f1, ..., fn]`` is f1 ∧ \.\.\. ∧ fn

  .. _constr-da-logic-types-disjunction-65549:

  `Disjunction <constr-da-logic-types-disjunction-65549_>`_ \[`Formula <type-da-logic-types-formula-34794_>`_ t\]

    For formulas f1, \.\.\., fn, ``Disjunction [f1, ..., fn]`` is f1 ∨ \.\.\. ∨ fn

  **instance** :ref:`Action <class-da-internal-prelude-action-68790>` `Formula <type-da-logic-types-formula-34794_>`_

  **instance** :ref:`Applicative <class-da-internal-prelude-applicative-9257>` `Formula <type-da-logic-types-formula-34794_>`_

  **instance** :ref:`Functor <class-ghc-base-functor-31205>` `Formula <type-da-logic-types-formula-34794_>`_

  **instance** :ref:`Eq <class-ghc-classes-eq-22713>` t \=\> :ref:`Eq <class-ghc-classes-eq-22713>` (`Formula <type-da-logic-types-formula-34794_>`_ t)

  **instance** :ref:`Ord <class-ghc-classes-ord-6395>` t \=\> :ref:`Ord <class-ghc-classes-ord-6395>` (`Formula <type-da-logic-types-formula-34794_>`_ t)

  **instance** :ref:`Show <class-ghc-show-show-65360>` t \=\> :ref:`Show <class-ghc-show-show-65360>` (`Formula <type-da-logic-types-formula-34794_>`_ t)

Functions
---------

.. _function-da-logic-ampampamp-55265:

`(&&&) <function-da-logic-ampampamp-55265_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``&&&`` is the ∧ operation of the boolean algebra of formulas, to
  be read as \"and\"

.. _function-da-logic-pipepipepipe-30747:

`(|||) <function-da-logic-pipepipepipe-30747_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``|||`` is the ∨ operation of the boolean algebra of formulas, to
  be read as \"or\"

.. _function-da-logic-true-31438:

`true <function-da-logic-true-31438_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t

  ``true`` is the 1 element of the boolean algebra of formulas,
  represented as an empty conjunction\.

.. _function-da-logic-false-99028:

`false <function-da-logic-false-99028_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t

  ``false`` is the 0 element of the boolean algebra of formulas,
  represented as an empty disjunction\.

.. _function-da-logic-neg-1597:

`neg <function-da-logic-neg-1597_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``neg`` is the ¬ (negation) operation of the boolean algebra of
  formulas\.

.. _function-da-logic-conj-82504:

`conj <function-da-logic-conj-82504_>`_
  \: \[`Formula <type-da-logic-types-formula-34794_>`_ t\] \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``conj`` is a list version of ``&&&``, enabled by the associativity
  of ∧\.

.. _function-da-logic-disj-92448:

`disj <function-da-logic-disj-92448_>`_
  \: \[`Formula <type-da-logic-types-formula-34794_>`_ t\] \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``disj`` is a list version of ``|||``, enabled by the associativity
  of ∨\.

.. _function-da-logic-frombool-36630:

`fromBool <function-da-logic-frombool-36630_>`_
  \: :ref:`Bool <type-ghc-types-bool-66265>` \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``fromBool`` converts ``True`` to ``true`` and ``False`` to ``false``\.

.. _function-da-logic-tonnf-87354:

`toNNF <function-da-logic-tonnf-87354_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``toNNF`` transforms a formula to negation normal form
  (see https\://en\.wikipedia\.org/wiki/Negation\_normal\_form)\.

.. _function-da-logic-todnf-90852:

`toDNF <function-da-logic-todnf-90852_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``toDNF`` turns a formula into disjunctive normal form\.
  (see https\://en\.wikipedia\.org/wiki/Disjunctive\_normal\_form)\.

.. _function-da-logic-traverse-17816:

`traverse <function-da-logic-traverse-17816_>`_
  \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f \=\> (t \-\> f s) \-\> `Formula <type-da-logic-types-formula-34794_>`_ t \-\> f (`Formula <type-da-logic-types-formula-34794_>`_ s)

  An implementation of ``traverse`` in the usual sense\.

.. _function-da-logic-zipformulas-28999:

`zipFormulas <function-da-logic-zipformulas-28999_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ s \-\> `Formula <type-da-logic-types-formula-34794_>`_ (t, s)

  ``zipFormulas`` takes to formulas of same shape, meaning only
  propositions are different and zips them up\.

.. _function-da-logic-substitute-65872:

`substitute <function-da-logic-substitute-65872_>`_
  \: (t \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Bool <type-ghc-types-bool-66265>`) \-\> `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``substitute`` takes a truth assignment and substitutes ``True`` or
  ``False`` into the respective places in a formula\.

.. _function-da-logic-reduce-40218:

`reduce <function-da-logic-reduce-40218_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t \-\> `Formula <type-da-logic-types-formula-34794_>`_ t

  ``reduce`` reduces a formula as far as possible by\:

  1. Removing any occurrences of ``true`` and ``false``;
  2. Removing directly nested Conjunctions and Disjunctions;
  3. Going to negation normal form\.

.. _function-da-logic-isbool-80820:

`isBool <function-da-logic-isbool-80820_>`_
  \: `Formula <type-da-logic-types-formula-34794_>`_ t \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Bool <type-ghc-types-bool-66265>`

  ``isBool`` attempts to convert a formula to a bool\. It satisfies
  ``isBool true == Some True`` and ``isBool false == Some False``\.
  Otherwise, it returns ``None``\.

.. _function-da-logic-interpret-88386:

`interpret <function-da-logic-interpret-88386_>`_
  \: (t \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Bool <type-ghc-types-bool-66265>`) \-\> `Formula <type-da-logic-types-formula-34794_>`_ t \-\> :ref:`Either <type-da-types-either-56020>` (`Formula <type-da-logic-types-formula-34794_>`_ t) :ref:`Bool <type-ghc-types-bool-66265>`

  ``interpret`` is a version of ``toBool`` that first substitutes using
  a truth function and then reduces as far as possible\.

.. _function-da-logic-substitutea-61566:

`substituteA <function-da-logic-substitutea-61566_>`_
  \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f \=\> (t \-\> f (:ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Bool <type-ghc-types-bool-66265>`)) \-\> `Formula <type-da-logic-types-formula-34794_>`_ t \-\> f (`Formula <type-da-logic-types-formula-34794_>`_ t)

  ``substituteA`` is a version of ``substitute`` that allows for truth
  values to be obtained from an action\.

.. _function-da-logic-interpreta-14928:

`interpretA <function-da-logic-interpreta-14928_>`_
  \: :ref:`Applicative <class-da-internal-prelude-applicative-9257>` f \=\> (t \-\> f (:ref:`Optional <type-da-internal-prelude-optional-37153>` :ref:`Bool <type-ghc-types-bool-66265>`)) \-\> `Formula <type-da-logic-types-formula-34794_>`_ t \-\> f (:ref:`Either <type-da-types-either-56020>` (`Formula <type-da-logic-types-formula-34794_>`_ t) :ref:`Bool <type-ghc-types-bool-66265>`)

  ``interpretA`` is a version of ``interpret`` that allows for truth
  values to be obtained form an action\.
