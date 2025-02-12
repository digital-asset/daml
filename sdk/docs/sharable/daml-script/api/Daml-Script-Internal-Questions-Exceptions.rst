.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-exceptions-16263:

Daml.Script.Internal.Questions.Exceptions
=========================================

Orphan Typeclass Instances
--------------------------

**instance** `ActionCatch <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-actioncatch-69238>`_ :ref:`Script <type-daml-script-internal-lowlevel-script-4781>`

**instance** `ActionThrow <https://docs.daml.com/daml/stdlib/DA-Exception.html#class-da-internal-exception-actionthrow-37623>`_ :ref:`Script <type-daml-script-internal-lowlevel-script-4781>`

**instance** `CanAssert <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-assert-canassert-67323>`_ :ref:`Script <type-daml-script-internal-lowlevel-script-4781>`

Data Types
----------

.. _type-daml-script-internal-questions-exceptions-catch-84605:

**data** `Catch <type-daml-script-internal-questions-exceptions-catch-84605_>`_

  .. _constr-daml-script-internal-questions-exceptions-catch-98214:

  `Catch <constr-daml-script-internal-questions-exceptions-catch-98214_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - act
         - () \-\> :ref:`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913>`
         -
       * - dummy
         - ()
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `Catch <type-daml-script-internal-questions-exceptions-catch-84605_>`_ (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_ x)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"act\" `Catch <type-daml-script-internal-questions-exceptions-catch-84605_>`_ (() \-\> :ref:`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913>`)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"dummy\" `Catch <type-daml-script-internal-questions-exceptions-catch-84605_>`_ ()

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"act\" `Catch <type-daml-script-internal-questions-exceptions-catch-84605_>`_ (() \-\> :ref:`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913>`)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"dummy\" `Catch <type-daml-script-internal-questions-exceptions-catch-84605_>`_ ()

.. _type-daml-script-internal-questions-exceptions-throw-53740:

**data** `Throw <type-daml-script-internal-questions-exceptions-throw-53740_>`_

  .. _constr-daml-script-internal-questions-exceptions-throw-78439:

  `Throw <constr-daml-script-internal-questions-exceptions-throw-78439_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - exc
         - `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `Throw <type-daml-script-internal-questions-exceptions-throw-53740_>`_ t

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"exc\" `Throw <type-daml-script-internal-questions-exceptions-throw-53740_>`_ `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"exc\" `Throw <type-daml-script-internal-questions-exceptions-throw-53740_>`_ `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_

Functions
---------

.. _function-daml-script-internal-questions-exceptions-trytoeither-58773:

`tryToEither <function-daml-script-internal-questions-exceptions-trytoeither-58773_>`_
  \: (() \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` t) \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_ t)

  Named version of the ``try catch`` behaviour of Daml\-Script\.
  Note that this is no more powerful than ``try catch`` in daml\-script, and will not catch exceptions in submissions\.
  (Use ``trySubmit`` for this)
  Input computation is deferred to catch pure exceptions

.. _function-daml-script-internal-questions-exceptions-throwanyexception-70957:

`throwAnyException <function-daml-script-internal-questions-exceptions-throwanyexception-70957_>`_
  \: `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` t

