.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-transactiontree-7318:

Daml.Script.Internal.Questions.TransactionTree
==============================================

Data Types
----------

.. _type-daml-script-internal-questions-transactiontree-created-98301:

**data** `Created <type-daml-script-internal-questions-transactiontree-created-98301_>`_

  .. _constr-daml-script-internal-questions-transactiontree-created-79356:

  `Created <constr-daml-script-internal-questions-transactiontree-created-79356_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - :ref:`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399>`
         -
       * - argument
         - `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `Created <type-daml-script-internal-questions-transactiontree-created-98301_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"argument\" `Created <type-daml-script-internal-questions-transactiontree-created-98301_>`_ `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `Created <type-daml-script-internal-questions-transactiontree-created-98301_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399>`

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"argument\" `Created <type-daml-script-internal-questions-transactiontree-created-98301_>`_ `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `Created <type-daml-script-internal-questions-transactiontree-created-98301_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399>`

.. _type-daml-script-internal-questions-transactiontree-createdindexpayload-52051:

**data** `CreatedIndexPayload <type-daml-script-internal-questions-transactiontree-createdindexpayload-52051_>`_ t

  .. _constr-daml-script-internal-questions-transactiontree-createdindexpayload-17054:

  `CreatedIndexPayload <constr-daml-script-internal-questions-transactiontree-createdindexpayload-17054_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - templateId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - offset
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"offset\" (`CreatedIndexPayload <type-daml-script-internal-questions-transactiontree-createdindexpayload-52051_>`_ t) `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"templateId\" (`CreatedIndexPayload <type-daml-script-internal-questions-transactiontree-createdindexpayload-52051_>`_ t) `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"offset\" (`CreatedIndexPayload <type-daml-script-internal-questions-transactiontree-createdindexpayload-52051_>`_ t) `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"templateId\" (`CreatedIndexPayload <type-daml-script-internal-questions-transactiontree-createdindexpayload-52051_>`_ t) `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-transactiontree-exercised-22057:

**data** `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_

  .. _constr-daml-script-internal-questions-transactiontree-exercised-56388:

  `Exercised <constr-daml-script-internal-questions-transactiontree-exercised-56388_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - :ref:`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399>`
         -
       * - choice
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - argument
         - `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_
         -
       * - childEvents
         - \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]
         -

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"argument\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"childEvents\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"choice\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399>`

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"argument\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ `AnyChoice <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"childEvents\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"choice\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399>`

.. _type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779:

**data** `ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t

  .. _constr-daml-script-internal-questions-transactiontree-exercisedindexpayload-97386:

  `ExercisedIndexPayload <constr-daml-script-internal-questions-transactiontree-exercisedindexpayload-97386_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - templateId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - choice
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - offset
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -
       * - child
         - `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t
         -

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"child\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) (`TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"choice\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"offset\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"templateId\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"child\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) (`TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"choice\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"offset\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"templateId\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-transactiontree-transactiontree-91781:

**data** `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_

  .. _constr-daml-script-internal-questions-transactiontree-transactiontree-56296:

  `TransactionTree <constr-daml-script-internal-questions-transactiontree-transactiontree-56296_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - rootEvents
         - \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]
         -
       * - timeBoundaries
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Range <https://docs.daml.com/daml/stdlib/DA-Time.html#type-da-time-range-81490>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` :ref:`Submit <type-daml-script-internal-questions-submit-submit-31549>` \[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_)\]

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"continue\" (:ref:`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688>` a) (\[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_)\] \-\> a)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"rootEvents\" `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_ \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"timeBoundaries\" `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Range <https://docs.daml.com/daml/stdlib/DA-Time.html#type-da-time-range-81490>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"continue\" (:ref:`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688>` a) (\[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_)\] \-\> a)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"rootEvents\" `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_ \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"timeBoundaries\" `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Range <https://docs.daml.com/daml/stdlib/DA-Time.html#type-da-time-range-81490>`_)

.. _type-daml-script-internal-questions-transactiontree-treeevent-1267:

**data** `TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_

  .. _constr-daml-script-internal-questions-transactiontree-createdevent-60119:

  `CreatedEvent <constr-daml-script-internal-questions-transactiontree-createdevent-60119_>`_ `Created <type-daml-script-internal-questions-transactiontree-created-98301_>`_


  .. _constr-daml-script-internal-questions-transactiontree-exercisedevent-2627:

  `ExercisedEvent <constr-daml-script-internal-questions-transactiontree-exercisedevent-2627_>`_ `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_


  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"childEvents\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"rootEvents\" `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_ \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"childEvents\" `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_ \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"rootEvents\" `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_ \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]

.. _type-daml-script-internal-questions-transactiontree-treeindex-21327:

**data** `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t

  .. _constr-daml-script-internal-questions-transactiontree-createdindex-88223:

  `CreatedIndex <constr-daml-script-internal-questions-transactiontree-createdindex-88223_>`_ (`CreatedIndexPayload <type-daml-script-internal-questions-transactiontree-createdindexpayload-52051_>`_ t)


  .. _constr-daml-script-internal-questions-transactiontree-exercisedindex-22399:

  `ExercisedIndex <constr-daml-script-internal-questions-transactiontree-exercisedindex-22399_>`_ (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t)


  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"child\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) (`TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"child\" (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t) (`TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t)

Functions
---------

.. _function-daml-script-internal-questions-transactiontree-fromtree-1340:

`fromTree <function-daml-script-internal-questions-transactiontree-fromtree-1340_>`_
  \: `Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t \=\> `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t \-\> `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t

  Finds the contract id of an event within a tree given a tree index
  Tree indices are created using the ``created(N)`` and ``exercised(N)`` builders
  which allow building \"paths\" within a transaction to a create node
  For example, ``exercisedN @MyTemplate1 "MyChoice" 2 $ createdN @MyTemplate2 1``
  would find the ``ContractId MyTemplate2`` of the second (0 index) create event under
  the 3rd exercise event of ``MyChoice`` from ``MyTemplate1``

.. _function-daml-script-internal-questions-transactiontree-fromtreego-34976:

`fromTreeGo <function-daml-script-internal-questions-transactiontree-fromtreego-34976_>`_
  \: `Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t \=\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t \-\> \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\] \-\> `ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t

.. _function-daml-script-internal-questions-transactiontree-created-56097:

`created <function-daml-script-internal-questions-transactiontree-created-56097_>`_
  \: `HasTemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t

  Index for the first create event of a given template
  e\.g\. ``created @MyTemplate``

.. _function-daml-script-internal-questions-transactiontree-createdn-71930:

`createdN <function-daml-script-internal-questions-transactiontree-createdn-71930_>`_
  \: `HasTemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t

  Index for the Nth create event of a given template
  e\.g\. ``createdN 2 @MyTemplate``
  ``created = createdN 0``

.. _function-daml-script-internal-questions-transactiontree-exercised-13349:

`exercised <function-daml-script-internal-questions-transactiontree-exercised-13349_>`_
  \: `HasTemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t' \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t'

  Index for the first exercise of a given choice on a given template
  e\.g\. \`exercised @MyTemplate \"MyChoice\"

.. _function-daml-script-internal-questions-transactiontree-exercisedn-70910:

`exercisedN <function-daml-script-internal-questions-transactiontree-exercisedn-70910_>`_
  \: `HasTemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t' \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t'

  Index for the Nth exercise of a given choice on a given template
  e\.g\. ``exercisedN @MyTemplate "MyChoice" 2 ``exercised c \= exercisedN c 0\`

