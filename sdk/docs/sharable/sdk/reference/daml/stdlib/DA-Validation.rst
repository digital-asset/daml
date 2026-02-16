.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-validation-69700:

DA.Validation
=============

``Validation`` type and associated functions\.

Data Types
----------

.. _type-da-validation-types-validation-39644:

**data** `Validation <type-da-validation-types-validation-39644_>`_ err a

  A ``Validation`` represents eithor a non\-empty list of errors, or a successful value\.
  This generalizes ``Either`` to allow more than one error to be collected\.

  .. _constr-da-validation-types-errors-73825:

  `Errors <constr-da-validation-types-errors-73825_>`_ (:ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` err)


  .. _constr-da-validation-types-success-12286:

  `Success <constr-da-validation-types-success-12286_>`_ a


  **instance** :ref:`Foldable <class-da-foldable-foldable-25994>` (`Validation <type-da-validation-types-validation-39644_>`_ err)

  **instance** :ref:`Applicative <class-da-internal-prelude-applicative-9257>` (`Validation <type-da-validation-types-validation-39644_>`_ err)

  **instance** :ref:`Semigroup <class-da-internal-prelude-semigroup-78998>` (`Validation <type-da-validation-types-validation-39644_>`_ err a)

  **instance** :ref:`Traversable <class-da-traversable-traversable-18144>` (`Validation <type-da-validation-types-validation-39644_>`_ err)

  **instance** (Serializable err, Serializable a) \=\> Serializable (`Validation <type-da-validation-types-validation-39644_>`_ err a)

  **instance** :ref:`Functor <class-ghc-base-functor-31205>` (`Validation <type-da-validation-types-validation-39644_>`_ err)

  **instance** (:ref:`Eq <class-ghc-classes-eq-22713>` err, :ref:`Eq <class-ghc-classes-eq-22713>` a) \=\> :ref:`Eq <class-ghc-classes-eq-22713>` (`Validation <type-da-validation-types-validation-39644_>`_ err a)

  **instance** (:ref:`Show <class-ghc-show-show-65360>` err, :ref:`Show <class-ghc-show-show-65360>` a) \=\> :ref:`Show <class-ghc-show-show-65360>` (`Validation <type-da-validation-types-validation-39644_>`_ err a)

Functions
---------

.. _function-da-validation-invalid-71114:

`invalid <function-da-validation-invalid-71114_>`_
  \: err \-\> `Validation <type-da-validation-types-validation-39644_>`_ err a

  Fail for the given reason\.

.. _function-da-validation-ok-57346:

`ok <function-da-validation-ok-57346_>`_
  \: a \-\> `Validation <type-da-validation-types-validation-39644_>`_ err a

  Succeed with the given value\.

.. _function-da-validation-validate-15676:

`validate <function-da-validation-validate-15676_>`_
  \: :ref:`Either <type-da-types-either-56020>` err a \-\> `Validation <type-da-validation-types-validation-39644_>`_ err a

  Turn an ``Either`` into a ``Validation``\.

.. _function-da-validation-run-73024:

`run <function-da-validation-run-73024_>`_
  \: `Validation <type-da-validation-types-validation-39644_>`_ err a \-\> :ref:`Either <type-da-types-either-56020>` (:ref:`NonEmpty <type-da-nonempty-types-nonempty-16010>` err) a

  Convert a ``Validation err a`` value into an ``Either``,
  taking the non\-empty list of errors as the left value\.

.. _function-da-validation-run1-16566:

`run1 <function-da-validation-run1-16566_>`_
  \: `Validation <type-da-validation-types-validation-39644_>`_ err a \-\> :ref:`Either <type-da-types-either-56020>` err a

  Convert a ``Validation err a`` value into an ``Either``,
  taking just the first error as the left value\.

.. _function-da-validation-runwithdefault-81974:

`runWithDefault <function-da-validation-runwithdefault-81974_>`_
  \: a \-\> `Validation <type-da-validation-types-validation-39644_>`_ err a \-\> a

  Run a ``Validation err a`` with a default value in case of errors\.

.. _function-da-validation-ltwhatgt-24976:

`(<?>) <function-da-validation-ltwhatgt-24976_>`_
  \: :ref:`Optional <type-da-internal-prelude-optional-37153>` b \-\> err \-\> `Validation <type-da-validation-types-validation-39644_>`_ err b

  Convert an ``Optional t`` into a ``Validation err t``, or
  more generally into an ``m t`` for any ``ActionFail`` type ``m``\.
