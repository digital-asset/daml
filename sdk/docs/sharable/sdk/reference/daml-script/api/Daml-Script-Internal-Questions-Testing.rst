.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-testing-42410:

Daml.Script.Internal.Questions.Testing
======================================

Data Types
----------

.. _type-daml-script-internal-questions-testing-commandname-12991:

**data** `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_

  .. _constr-daml-script-internal-questions-testing-commandname-12826:

  `CommandName <constr-daml-script-internal-questions-testing-commandname-12826_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getCommandName
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"commandName\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"getCommandName\" `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"commandName\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"getCommandName\" `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-testing-errorclassname-49861:

**data** `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_

  .. _constr-daml-script-internal-questions-testing-errorclassname-42862:

  `ErrorClassName <constr-daml-script-internal-questions-testing-errorclassname-42862_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getErrorClassName
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorClassName\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"getErrorClassName\" `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorClassName\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"getErrorClassName\" `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-testing-errormessage-78991:

**data** `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_

  .. _constr-daml-script-internal-questions-testing-errormessage-24784:

  `ErrorMessage <constr-daml-script-internal-questions-testing-errormessage-24784_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getErrorMessage
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorMessage\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"getErrorMessage\" `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorMessage\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"getErrorMessage\" `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-testing-failedcmd-88074:

**data** `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_

  .. _constr-daml-script-internal-questions-testing-failedcmd-77803:

  `FailedCmd <constr-daml-script-internal-questions-testing-failedcmd-77803_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - commandName
         - `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_
         -
       * - errorClassName
         - `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_
         -
       * - errorMessage
         - `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_
         -

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"commandName\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorClassName\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorMessage\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"commandName\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorClassName\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorMessage\" `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_

.. _type-daml-script-internal-questions-testing-trycommands-91696:

**data** `TryCommands <type-daml-script-internal-questions-testing-trycommands-91696_>`_

  .. _constr-daml-script-internal-questions-testing-trycommands-69201:

  `TryCommands <constr-daml-script-internal-questions-testing-trycommands-69201_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - act
         - :ref:`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913>`
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `TryCommands <type-daml-script-internal-questions-testing-trycommands-91696_>`_ (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ (`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_) x)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"act\" `TryCommands <type-daml-script-internal-questions-testing-trycommands-91696_>`_ :ref:`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913>`

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"act\" `TryCommands <type-daml-script-internal-questions-testing-trycommands-91696_>`_ :ref:`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913>`

Functions
---------

.. _function-daml-script-internal-questions-testing-tupletofailedcmd-4378:

`tupleToFailedCmd <function-daml-script-internal-questions-testing-tupletofailedcmd-4378_>`_
  \: (`Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_, `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_) \-\> `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_

.. _function-daml-script-internal-questions-testing-trycommands-17332:

`tryCommands <function-daml-script-internal-questions-testing-trycommands-17332_>`_
  \: :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ a)

.. _function-daml-script-internal-questions-testing-liftfailedcommandtofailurestatus-62416:

`liftFailedCommandToFailureStatus <function-daml-script-internal-questions-testing-liftfailedcommandtofailurestatus-62416_>`_
  \: :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a

