.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-commands-84337:

Daml.Script.Internal.Questions.Commands
=======================================

Data Types
----------

.. _type-daml-script-internal-questions-commands-command-31059:

**data** `Command <type-daml-script-internal-questions-commands-command-31059_>`_

  .. _constr-daml-script-internal-questions-commands-create-81100:

  `Create <constr-daml-script-internal-questions-commands-create-81100_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - argC
         - `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -

  .. _constr-daml-script-internal-questions-commands-exercise-71428:

  `Exercise <constr-daml-script-internal-questions-commands-exercise-71428_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - tplId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - cId
         - `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ()
         -
       * - argE
         - `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_
         -

  .. _constr-daml-script-internal-questions-commands-exercisebykey-33871:

  `ExerciseByKey <constr-daml-script-internal-questions-commands-exercisebykey-33871_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - tplId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - keyE
         - `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
         -
       * - argE
         - `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_
         -

  .. _constr-daml-script-internal-questions-commands-createandexercise-99660:

  `CreateAndExercise <constr-daml-script-internal-questions-commands-createandexercise-99660_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - tplArgCE
         - `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -
       * - choiceArgCE
         - `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_
         -

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"argC\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"argE\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"cId\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"choiceArgCE\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"command\" `CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_ `Command <type-daml-script-internal-questions-commands-command-31059_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"keyE\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"tplArgCE\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"tplId\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"argC\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"argE\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"cId\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"choiceArgCE\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"command\" `CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_ `Command <type-daml-script-internal-questions-commands-command-31059_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"keyE\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"tplArgCE\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"tplId\" `Command <type-daml-script-internal-questions-commands-command-31059_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-commands-commandresult-15750:

**data** `CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_

  .. _constr-daml-script-internal-questions-commands-createresult-63989:

  `CreateResult <constr-daml-script-internal-questions-commands-createresult-63989_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())


  .. _constr-daml-script-internal-questions-commands-exerciseresult-90025:

  `ExerciseResult <constr-daml-script-internal-questions-commands-exerciseresult-90025_>`_ :ref:`LedgerValue <type-daml-script-internal-lowlevel-ledgervalue-66913>`


  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` :ref:`Submit <type-daml-script-internal-questions-submit-submit-31549>` \[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\]

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"continue\" (`Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a) (\[`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_\] \-\> a)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"continue\" (:ref:`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688>` a) (\[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\] \-\> a)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"continue\" (`Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a) (\[`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_\] \-\> a)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"continue\" (:ref:`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688>` a) (\[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\] \-\> a)

.. _type-daml-script-internal-questions-commands-commandwithmeta-50560:

**data** `CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_

  .. _constr-daml-script-internal-questions-commands-commandwithmeta-90647:

  `CommandWithMeta <constr-daml-script-internal-questions-commands-commandwithmeta-90647_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - command
         - `Command <type-daml-script-internal-questions-commands-command-31059_>`_
         -
       * - explicitPackageId
         - `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_
         -

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"command\" `CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_ `Command <type-daml-script-internal-questions-commands-command-31059_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"commands\" (`Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a) \[`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"explicitPackageId\" `CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_ `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sCommands\" :ref:`Submission <type-daml-script-internal-questions-submit-submission-45309>` \[`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"command\" `CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_ `Command <type-daml-script-internal-questions-commands-command-31059_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"commands\" (`Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a) \[`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"explicitPackageId\" `CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_ `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sCommands\" :ref:`Submission <type-daml-script-internal-questions-submit-submission-45309>` \[`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_\]

.. _type-daml-script-internal-questions-commands-commands-79301:

**data** `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a

  This is used to build up the commands send as part of ``submit``\.
  If you enable the ``ApplicativeDo`` extension by adding
  ``{-# LANGUAGE ApplicativeDo #-}`` at the top of your file, you can
  use ``do``\-notation but the individual commands must not depend
  on each other and the last statement in a ``do`` block
  must be of the form ``return expr`` or ``pure expr``\.

  .. _constr-daml-script-internal-questions-commands-commands-42332:

  `Commands <constr-daml-script-internal-questions-commands-commands-42332_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - commands
         - \[`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_\]
         -
       * - continue
         - \[`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_\] \-\> a
         -

  **instance** `Functor <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-base-functor-31205>`_ `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_

  **instance** `Applicative <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257>`_ `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"commands\" (`Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a) \[`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"continue\" (`Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a) (\[`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_\] \-\> a)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"commands\" (`Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a) \[`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"continue\" (`Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a) (\[`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750_>`_\] \-\> a)

.. _type-daml-script-internal-questions-commands-disclosure-40298:

**data** `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_

  .. _constr-daml-script-internal-questions-commands-disclosure-14083:

  `Disclosure <constr-daml-script-internal-questions-commands-disclosure-14083_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - templateId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - contractId
         - `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ()
         -
       * - blob
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_

  **instance** `Ord <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395>`_ `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"blob\" `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"sDisclosures\" :ref:`Submission <type-daml-script-internal-questions-submit-submission-45309>` \[`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"soDisclosures\" :ref:`SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692>` \[`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"templateId\" `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"blob\" `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"sDisclosures\" :ref:`Submission <type-daml-script-internal-questions-submit-submission-45309>` \[`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"soDisclosures\" :ref:`SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692>` \[`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"templateId\" `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

Functions
---------

.. _function-daml-script-internal-questions-commands-expectedcommandresults-34566:

`expectedCommandResults <function-daml-script-internal-questions-commands-expectedcommandresults-34566_>`_
  \: `Command <type-daml-script-internal-questions-commands-command-31059_>`_ \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

.. _function-daml-script-internal-questions-commands-expectedcommandsresults-87448:

`expectedCommandsResults <function-daml-script-internal-questions-commands-expectedcommandsresults-87448_>`_
  \: \[`CommandWithMeta <type-daml-script-internal-questions-commands-commandwithmeta-50560_>`_\] \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

.. _function-daml-script-internal-questions-commands-createcmd-46830:

`createCmd <function-daml-script-internal-questions-commands-createcmd-46830_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t)

  Create a contract of the given template\.

.. _function-daml-script-internal-questions-commands-exercisecmd-7438:

`exerciseCmd <function-daml-script-internal-questions-commands-exercisecmd-7438_>`_
  \: `Choice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r \=\> `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Exercise a choice on the given contract\.

.. _function-daml-script-internal-questions-commands-exercisebykeycmd-80697:

`exerciseByKeyCmd <function-daml-script-internal-questions-commands-exercisebykeycmd-80697_>`_
  \: (`TemplateKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `Choice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r) \=\> k \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Exercise a choice on the contract with the given key\.

.. _function-daml-script-internal-questions-commands-createandexercisewithcidcmd-21289:

`createAndExerciseWithCidCmd <function-daml-script-internal-questions-commands-createandexercisewithcidcmd-21289_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, r)

  Create a contract and exercise a choice on it in the same transaction, returns the created ContractId, and the choice result\.

.. _function-daml-script-internal-questions-commands-createandexercisecmd-8600:

`createAndExerciseCmd <function-daml-script-internal-questions-commands-createandexercisecmd-8600_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Create a contract and exercise a choice on it in the same transaction, returns only the choice result\.

.. _function-daml-script-internal-questions-commands-createexactcmd-86998:

`createExactCmd <function-daml-script-internal-questions-commands-createexactcmd-86998_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t)

  Create a contract of the given template, using the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-exerciseexactcmd-18398:

`exerciseExactCmd <function-daml-script-internal-questions-commands-exerciseexactcmd-18398_>`_
  \: `Choice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r \=\> `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Exercise a choice on the given contract, using the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-exercisebykeyexactcmd-4555:

`exerciseByKeyExactCmd <function-daml-script-internal-questions-commands-exercisebykeyexactcmd-4555_>`_
  \: (`TemplateKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `Choice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r) \=\> k \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Exercise a choice on the contract with the given key, using the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-createandexercisewithcidexactcmd-15363:

`createAndExerciseWithCidExactCmd <function-daml-script-internal-questions-commands-createandexercisewithcidexactcmd-15363_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, r)

  Create a contract and exercise a choice on it in the same transaction, returns the created ContractId, and the choice result\.
  Uses the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-createandexerciseexactcmd-54956:

`createAndExerciseExactCmd <function-daml-script-internal-questions-commands-createandexerciseexactcmd-54956_>`_
  \: (`Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Create a contract and exercise a choice on it in the same transaction, returns only the choice result\.

.. _function-daml-script-internal-questions-commands-archivecmd-47203:

`archiveCmd <function-daml-script-internal-questions-commands-archivecmd-47203_>`_
  \: `Choice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t `Archive <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-archive-15178>`_ () \=\> `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ ()

  Archive the given contract\.

  ``archiveCmd cid`` is equivalent to ``exerciseCmd cid Archive``\.

