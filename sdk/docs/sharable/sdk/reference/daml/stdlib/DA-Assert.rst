.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-assert-92761:

DA.Assert
=========

Functions
---------

.. _function-da-assert-asserteq-7135:

`assertEq <function-da-assert-asserteq-7135_>`_
  \: (:ref:`CanAssert <class-da-internal-assert-canassert-67323>` m, :ref:`Show <class-ghc-show-show-65360>` a, :ref:`Eq <class-ghc-classes-eq-22713>` a) \=\> a \-\> a \-\> m ()

  Check two values for equality\. If they're not equal,
  fail with a message\.

.. _function-da-assert-eqeqeq-18699:

`(===) <function-da-assert-eqeqeq-18699_>`_
  \: (:ref:`CanAssert <class-da-internal-assert-canassert-67323>` m, :ref:`Show <class-ghc-show-show-65360>` a, :ref:`Eq <class-ghc-classes-eq-22713>` a) \=\> a \-\> a \-\> m ()

  Infix version of ``assertEq``\.

.. _function-da-assert-assertnoteq-28771:

`assertNotEq <function-da-assert-assertnoteq-28771_>`_
  \: (:ref:`CanAssert <class-da-internal-assert-canassert-67323>` m, :ref:`Show <class-ghc-show-show-65360>` a, :ref:`Eq <class-ghc-classes-eq-22713>` a) \=\> a \-\> a \-\> m ()

  Check two values for inequality\. If they're equal,
  fail with a message\.

.. _function-da-assert-eqslasheq-37517:

`(=/=) <function-da-assert-eqslasheq-37517_>`_
  \: (:ref:`CanAssert <class-da-internal-assert-canassert-67323>` m, :ref:`Show <class-ghc-show-show-65360>` a, :ref:`Eq <class-ghc-classes-eq-22713>` a) \=\> a \-\> a \-\> m ()

  Infix version of ``assertNotEq``\.

.. _function-da-assert-assertaftermsg-14090:

`assertAfterMsg <function-da-assert-assertaftermsg-14090_>`_
  \: (:ref:`CanAssert <class-da-internal-assert-canassert-67323>` m, :ref:`HasTime <class-da-internal-lf-hastime-96546>` m) \=\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Time <type-da-internal-lf-time-63886>` \-\> m ()

  Check whether the given time is in the future\. If it's not,
  abort with a message\.

.. _function-da-assert-assertbeforemsg-56514:

`assertBeforeMsg <function-da-assert-assertbeforemsg-56514_>`_
  \: (:ref:`CanAssert <class-da-internal-assert-canassert-67323>` m, :ref:`HasTime <class-da-internal-lf-hastime-96546>` m) \=\> :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Time <type-da-internal-lf-time-63886>` \-\> m ()

  Check whether the given time is in the past\. If it's not,
  abort with a message\.

.. _function-da-assert-assertwithindeadline-85580:

`assertWithinDeadline <function-da-assert-assertwithindeadline-85580_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Time <type-da-internal-lf-time-63886>` \-\> :ref:`Update <type-da-internal-lf-update-68072>` ()

  Check whether the ledger time of the transaction is strictly before the given deadline\.
  If it's not, abort with a message\.

.. _function-da-assert-assertdeadlineexceeded-21600:

`assertDeadlineExceeded <function-da-assert-assertdeadlineexceeded-21600_>`_
  \: :ref:`Text <type-ghc-types-text-51952>` \-\> :ref:`Time <type-da-internal-lf-time-63886>` \-\> :ref:`Update <type-da-internal-lf-update-68072>` ()

  Check whether the ledger time of the transaction is at or after the given deadline\.
  If it's not, abort with a message\.
