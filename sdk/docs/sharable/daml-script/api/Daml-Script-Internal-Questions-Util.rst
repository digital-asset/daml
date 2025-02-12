.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-util-65223:

Daml.Script.Internal.Questions.Util
===================================

Data Types
----------

.. _type-daml-script-internal-questions-util-anycontractid-11399:

**data** `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  .. _constr-daml-script-internal-questions-util-anycontractid-12200:

  `AnyContractId <constr-daml-script-internal-questions-util-anycontractid-12200_>`_

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

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"additionalInfoCid\" :ref:`ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199>` `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" :ref:`Created <type-daml-script-internal-questions-transactiontree-created-98301>` `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" :ref:`Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057>` `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"localVerdictLockedContracts\" :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` \[`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"templateId\" `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"additionalInfoCid\" :ref:`ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199>` `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" :ref:`Created <type-daml-script-internal-questions-transactiontree-created-98301>` `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" :ref:`Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057>` `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ())

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"localVerdictLockedContracts\" :ref:`SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284>` \[`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"templateId\" `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

Functions
---------

.. _function-daml-script-internal-questions-util-fromanycontractid-11435:

`fromAnyContractId <function-daml-script-internal-questions-util-fromanycontractid-11435_>`_
  \: `Template <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t \=\> `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_ \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`ContractId <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t)

