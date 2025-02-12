.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-da-exception-55791:

DA.Exception
============

Exception handling in Daml\.

Typeclasses
-----------

.. _class-da-internal-exception-hasthrow-30284:

**class** `HasThrow <class-da-internal-exception-hasthrow-30284_>`_ e **where**

  Part of the ``Exception`` constraint\.

  .. _function-da-internal-exception-throwpure-97636:

  `throwPure <function-da-internal-exception-throwpure-97636_>`_
    \: e \-\> t

    Throw exception in a pure context\.

  **instance** `HasThrow <class-da-internal-exception-hasthrow-30284_>`_ `ArithmeticError <type-da-exception-arithmeticerror-arithmeticerror-68828_>`_

  **instance** `HasThrow <class-da-internal-exception-hasthrow-30284_>`_ `AssertionFailed <type-da-exception-assertionfailed-assertionfailed-69740_>`_

  **instance** `HasThrow <class-da-internal-exception-hasthrow-30284_>`_ `GeneralError <type-da-exception-generalerror-generalerror-5800_>`_

  **instance** `HasThrow <class-da-internal-exception-hasthrow-30284_>`_ `PreconditionFailed <type-da-exception-preconditionfailed-preconditionfailed-61218_>`_

.. _class-da-internal-exception-hasmessage-3179:

**class** `HasMessage <class-da-internal-exception-hasmessage-3179_>`_ e **where**

  Part of the ``Exception`` constraint\.

  .. _function-da-internal-exception-message-17317:

  `message <function-da-internal-exception-message-17317_>`_
    \: e \-\> :ref:`Text <type-ghc-types-text-51952>`

    Get the error message associated with an exception\.

  **instance** `HasMessage <class-da-internal-exception-hasmessage-3179_>`_ :ref:`AnyException <type-da-internal-lf-anyexception-7004>`

  **instance** `HasMessage <class-da-internal-exception-hasmessage-3179_>`_ `ArithmeticError <type-da-exception-arithmeticerror-arithmeticerror-68828_>`_

  **instance** `HasMessage <class-da-internal-exception-hasmessage-3179_>`_ `AssertionFailed <type-da-exception-assertionfailed-assertionfailed-69740_>`_

  **instance** `HasMessage <class-da-internal-exception-hasmessage-3179_>`_ `GeneralError <type-da-exception-generalerror-generalerror-5800_>`_

  **instance** `HasMessage <class-da-internal-exception-hasmessage-3179_>`_ `PreconditionFailed <type-da-exception-preconditionfailed-preconditionfailed-61218_>`_

.. _class-da-internal-exception-hastoanyexception-55973:

**class** `HasToAnyException <class-da-internal-exception-hastoanyexception-55973_>`_ e **where**

  Part of the ``Exception`` constraint\.

  .. _function-da-internal-exception-toanyexception-88127:

  `toAnyException <function-da-internal-exception-toanyexception-88127_>`_
    \: e \-\> :ref:`AnyException <type-da-internal-lf-anyexception-7004>`

    Convert an exception type to AnyException\.

  **instance** `HasToAnyException <class-da-internal-exception-hastoanyexception-55973_>`_ :ref:`AnyException <type-da-internal-lf-anyexception-7004>`

  **instance** `HasToAnyException <class-da-internal-exception-hastoanyexception-55973_>`_ `ArithmeticError <type-da-exception-arithmeticerror-arithmeticerror-68828_>`_

  **instance** `HasToAnyException <class-da-internal-exception-hastoanyexception-55973_>`_ `AssertionFailed <type-da-exception-assertionfailed-assertionfailed-69740_>`_

  **instance** `HasToAnyException <class-da-internal-exception-hastoanyexception-55973_>`_ `GeneralError <type-da-exception-generalerror-generalerror-5800_>`_

  **instance** `HasToAnyException <class-da-internal-exception-hastoanyexception-55973_>`_ `PreconditionFailed <type-da-exception-preconditionfailed-preconditionfailed-61218_>`_

.. _class-da-internal-exception-hasfromanyexception-16788:

**class** `HasFromAnyException <class-da-internal-exception-hasfromanyexception-16788_>`_ e **where**

  Part of the ``Exception`` constraint\.

  .. _function-da-internal-exception-fromanyexception-70766:

  `fromAnyException <function-da-internal-exception-fromanyexception-70766_>`_
    \: :ref:`AnyException <type-da-internal-lf-anyexception-7004>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` e

    Convert an AnyException back to the underlying exception type, if possible\.

  **instance** `HasFromAnyException <class-da-internal-exception-hasfromanyexception-16788_>`_ :ref:`AnyException <type-da-internal-lf-anyexception-7004>`

  **instance** `HasFromAnyException <class-da-internal-exception-hasfromanyexception-16788_>`_ `ArithmeticError <type-da-exception-arithmeticerror-arithmeticerror-68828_>`_

  **instance** `HasFromAnyException <class-da-internal-exception-hasfromanyexception-16788_>`_ `AssertionFailed <type-da-exception-assertionfailed-assertionfailed-69740_>`_

  **instance** `HasFromAnyException <class-da-internal-exception-hasfromanyexception-16788_>`_ `GeneralError <type-da-exception-generalerror-generalerror-5800_>`_

  **instance** `HasFromAnyException <class-da-internal-exception-hasfromanyexception-16788_>`_ `PreconditionFailed <type-da-exception-preconditionfailed-preconditionfailed-61218_>`_

.. _class-da-internal-exception-actionthrow-37623:

**class** :ref:`Action <class-da-internal-prelude-action-68790>` m \=\> `ActionThrow <class-da-internal-exception-actionthrow-37623_>`_ m **where**

  Action type in which ``throw`` is supported\.

  .. _function-da-internal-exception-throw-28546:

  `throw <function-da-internal-exception-throw-28546_>`_
    \: `Exception <type-da-internal-exception-exception-4133_>`_ e \=\> e \-\> m t

  **instance** `ActionThrow <class-da-internal-exception-actionthrow-37623_>`_ :ref:`Update <type-da-internal-lf-update-68072>`

.. _class-da-internal-exception-actioncatch-69238:

**class** `ActionThrow <class-da-internal-exception-actionthrow-37623_>`_ m \=\> `ActionCatch <class-da-internal-exception-actioncatch-69238_>`_ m **where**

  Action type in which ``try ... catch ...`` is supported\.

  .. _function-da-internal-exception-trycatch-24973:

  `_tryCatch <function-da-internal-exception-trycatch-24973_>`_
    \: (() \-\> m t) \-\> (:ref:`AnyException <type-da-internal-lf-anyexception-7004>` \-\> :ref:`Optional <type-da-internal-prelude-optional-37153>` (m t)) \-\> m t

    Handle an exception\. Use the ``try ... catch ...`` syntax
    instead of calling this method directly\.

  **instance** `ActionCatch <class-da-internal-exception-actioncatch-69238_>`_ :ref:`Update <type-da-internal-lf-update-68072>`

Data Types
----------

.. _type-da-internal-exception-exception-4133:

**type** `Exception <type-da-internal-exception-exception-4133_>`_ e
  \= (`HasThrow <class-da-internal-exception-hasthrow-30284_>`_ e, `HasMessage <class-da-internal-exception-hasmessage-3179_>`_ e, `HasToAnyException <class-da-internal-exception-hastoanyexception-55973_>`_ e, `HasFromAnyException <class-da-internal-exception-hasfromanyexception-16788_>`_ e)

  Exception typeclass\. This should not be implemented directly,
  instead, use the ``exception`` syntax\.

.. _type-da-exception-arithmeticerror-arithmeticerror-68828:

**data** `ArithmeticError <type-da-exception-arithmeticerror-arithmeticerror-68828_>`_

  Exception raised by an arithmetic operation, such as divide\-by\-zero or overflow\.

  .. _constr-da-exception-arithmeticerror-arithmeticerror-83141:

  `ArithmeticError <constr-da-exception-arithmeticerror-arithmeticerror-83141_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - message
         - :ref:`Text <type-ghc-types-text-51952>`
         -

.. _type-da-exception-assertionfailed-assertionfailed-69740:

**data** `AssertionFailed <type-da-exception-assertionfailed-assertionfailed-69740_>`_

  Exception raised by assert functions in DA\.Assert

  .. _constr-da-exception-assertionfailed-assertionfailed-2357:

  `AssertionFailed <constr-da-exception-assertionfailed-assertionfailed-2357_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - message
         - :ref:`Text <type-ghc-types-text-51952>`
         -

.. _type-da-exception-generalerror-generalerror-5800:

**data** `GeneralError <type-da-exception-generalerror-generalerror-5800_>`_

  Exception raised by ``error``\.

  .. _constr-da-exception-generalerror-generalerror-9293:

  `GeneralError <constr-da-exception-generalerror-generalerror-9293_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - message
         - :ref:`Text <type-ghc-types-text-51952>`
         -

.. _type-da-exception-preconditionfailed-preconditionfailed-61218:

**data** `PreconditionFailed <type-da-exception-preconditionfailed-preconditionfailed-61218_>`_

  Exception raised when a contract is invalid, i\.e\. fails the ensure clause\.

  .. _constr-da-exception-preconditionfailed-preconditionfailed-18759:

  `PreconditionFailed <constr-da-exception-preconditionfailed-preconditionfailed-18759_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - message
         - :ref:`Text <type-ghc-types-text-51952>`
         -
