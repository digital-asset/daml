.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-submit-error-44839:

Daml.Script.Internal.Questions.Submit.Error
===========================================

Data Types
----------

.. _type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199:

**data** `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_

  Additional debugging info optionally provided by the ledger

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"actAs\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"additionalDebuggingInfo\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"additionalInfoCid\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ AnyContractId

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"effectiveAt\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"observers\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"readAs\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"actAs\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"additionalDebuggingInfo\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"additionalInfoCid\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ AnyContractId

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"effectiveAt\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"observers\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"readAs\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

.. _type-daml-script-internal-questions-submit-error-cryptoerrortype-71749:

**data** `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_

  .. _constr-daml-script-internal-questions-submit-error-malformedbyteencoding-79193:

  `MalformedByteEncoding <constr-daml-script-internal-questions-submit-error-malformedbyteencoding-79193_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - value
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-malformedkey-58536:

  `MalformedKey <constr-daml-script-internal-questions-submit-error-malformedkey-58536_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - keyValue
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-malformedsignature-13573:

  `MalformedSignature <constr-daml-script-internal-questions-submit-error-malformedsignature-13573_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - signatureValue
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"cryptoErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"keyValue\" `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"signatureValue\" `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"value\" `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"cryptoErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"keyValue\" `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"signatureValue\" `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"value\" `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-deverrortype-71788:

**data** `DevErrorType <type-daml-script-internal-questions-submit-error-deverrortype-71788_>`_

  Errors that will be promoted to SubmitError once stable \- code needs to be kept in sync with SubmitError\.scala

  .. _constr-daml-script-internal-questions-submit-error-choiceguardfailed-92292:

  `ChoiceGuardFailed <constr-daml-script-internal-questions-submit-error-choiceguardfailed-92292_>`_


  .. _constr-daml-script-internal-questions-submit-error-wronglytypedcontractsoft-93780:

  `WronglyTypedContractSoft <constr-daml-script-internal-questions-submit-error-wronglytypedcontractsoft-93780_>`_


  .. _constr-daml-script-internal-questions-submit-error-unknownnewfeature-96345:

  `UnknownNewFeature <constr-daml-script-internal-questions-submit-error-unknownnewfeature-96345_>`_

    This should never happen \- Update Scripts when you see this!

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `DevErrorType <type-daml-script-internal-questions-submit-error-deverrortype-71788_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"devErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `DevErrorType <type-daml-script-internal-questions-submit-error-deverrortype-71788_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"devErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `DevErrorType <type-daml-script-internal-questions-submit-error-deverrortype-71788_>`_

.. _type-daml-script-internal-questions-submit-error-submiterror-38284:

**data** `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_

  Errors that can be thrown by a command submission \- code needs to be kept in sync with SubmitError\.scala

  .. _constr-daml-script-internal-questions-submit-error-contractnotfound-62819:

  `ContractNotFound <constr-daml-script-internal-questions-submit-error-contractnotfound-62819_>`_

    Contract with given contract ID could not be found, and has never existed on this participant
    When run on Canton, there may be more than one contract ID, and additionalDebuggingInfo is always None
    On the other hand, when run on IDELedger, there is only ever one contract ID, and additionalDebuggingInfo is always Some

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - unknownContractIds
         - `NonEmpty <https://docs.daml.com/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         - Provided as text, as we do not know the template ID of a contract if the lookup fails
       * - additionalDebuggingInfo
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_
         - should always be None in Canton's case, see https\://github\.com/digital\-asset/daml/issues/17154

  .. _constr-daml-script-internal-questions-submit-error-contractkeynotfound-79659:

  `ContractKeyNotFound <constr-daml-script-internal-questions-submit-error-contractkeynotfound-79659_>`_

    Contract with given contract key could not be found

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractKey
         - `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-unresolvedpackagename-661:

  `UnresolvedPackageName <constr-daml-script-internal-questions-submit-error-unresolvedpackagename-661_>`_

    No vetted package with given package name could be found

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - packageName
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-authorizationerror-69757:

  `AuthorizationError <constr-daml-script-internal-questions-submit-error-authorizationerror-69757_>`_

    Generic authorization failure, included missing party authority, invalid signatories, etc\.

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - authorizationErrorMessage
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerror-69749:

  `DisclosedContractKeyHashingError <constr-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerror-69749_>`_

    Given disclosed contract key does not match the contract key of the contract on ledger\.

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - AnyContractId
         -
       * - expectedKey
         - `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
         -
       * - givenKeyHash
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-duplicatecontractkey-60422:

  `DuplicateContractKey <constr-daml-script-internal-questions-submit-error-duplicatecontractkey-60422_>`_

    Attempted to create a contract with a contract key that already exists

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - duplicateContractKey
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
         - Canton will often not provide this key, IDELedger will

  .. _constr-daml-script-internal-questions-submit-error-inconsistentcontractkey-74433:

  `InconsistentContractKey <constr-daml-script-internal-questions-submit-error-inconsistentcontractkey-74433_>`_

    Contract key lookup yielded different results

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractKey
         - `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-unhandledexception-86682:

  `UnhandledException <constr-daml-script-internal-questions-submit-error-unhandledexception-86682_>`_

    Unhandled user thrown exception

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - exc
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_
         - Errors more complex than simple records cannot currently be encoded over the grpc status\. Such errors will be missing here\.

  .. _constr-daml-script-internal-questions-submit-error-usererror-2902:

  `UserError <constr-daml-script-internal-questions-submit-error-usererror-2902_>`_

    Transaction failure due to abort/assert calls pre\-exceptions

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userErrorMessage
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-templatepreconditionviolated-57506:

  `TemplatePreconditionViolated <constr-daml-script-internal-questions-submit-error-templatepreconditionviolated-57506_>`_

    Failure due to false result from ``ensure``, strictly pre\-exception\.
    According to docs, not throwable with LF \>\= 1\.14\.
    On LF \>\= 1\.14, a failed ``ensure`` will result in a ``PreconditionFailed``
    exception wrapped in ``UnhandledException``\.

  .. _constr-daml-script-internal-questions-submit-error-createemptycontractkeymaintainers-30280:

  `CreateEmptyContractKeyMaintainers <constr-daml-script-internal-questions-submit-error-createemptycontractkeymaintainers-30280_>`_

    Attempted to create a contract with empty contract key maintainers

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - invalidTemplate
         - `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainers-19351:

  `FetchEmptyContractKeyMaintainers <constr-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainers-19351_>`_

    Attempted to fetch a contract with empty contract key maintainers

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - failedTemplateKey
         - `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-wronglytypedcontract-14384:

  `WronglyTypedContract <constr-daml-script-internal-questions-submit-error-wronglytypedcontract-14384_>`_

    Attempted to exercise/fetch a contract with the wrong template type

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - AnyContractId
         - Any contract Id of the actual contract
       * - expectedTemplateId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - actualTemplateId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-contractdoesnotimplementinterface-89439:

  `ContractDoesNotImplementInterface <constr-daml-script-internal-questions-submit-error-contractdoesnotimplementinterface-89439_>`_

    Attempted to use a contract as an interface that it does not implement

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - AnyContractId
         -
       * - templateId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - interfaceId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterface-51672:

  `ContractDoesNotImplementRequiringInterface <constr-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterface-51672_>`_

    Attempted to use a contract as a required interface that it does not implement

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - AnyContractId
         -
       * - templateId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - requiredInterfaceId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - requiringInterfaceId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-noncomparablevalues-97474:

  `NonComparableValues <constr-daml-script-internal-questions-submit-error-noncomparablevalues-97474_>`_

    Attempted to compare values that are not comparable

  .. _constr-daml-script-internal-questions-submit-error-contractidincontractkey-60542:

  `ContractIdInContractKey <constr-daml-script-internal-questions-submit-error-contractidincontractkey-60542_>`_

    Illegal Contract ID found in Contract Key

    (no fields)

  .. _constr-daml-script-internal-questions-submit-error-contractidcomparability-98492:

  `ContractIdComparability <constr-daml-script-internal-questions-submit-error-contractidcomparability-98492_>`_

    Attempted to compare a local and global contract ID with the same discriminator\. You're doing something very wrong

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - globalExistingContractId
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         - We do not know the template ID at time of comparison\.

  .. _constr-daml-script-internal-questions-submit-error-valuenesting-53471:

  `ValueNesting <constr-daml-script-internal-questions-submit-error-valuenesting-53471_>`_

    A value has been nested beyond a given depth limit

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - limit
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         - Nesting limit that was exceeded

  .. _constr-daml-script-internal-questions-submit-error-localverdictlockedcontracts-9414:

  `LocalVerdictLockedContracts <constr-daml-script-internal-questions-submit-error-localverdictlockedcontracts-9414_>`_

    The transaction refers to locked contracts which are in the process of being created, transferred, or
    archived by another transaction\. If the other transaction fails, this transaction could be successfully retried\.

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - localVerdictLockedContracts
         - \[AnyContractId\]
         - Locked contract ids

  .. _constr-daml-script-internal-questions-submit-error-localverdictlockedkeys-14824:

  `LocalVerdictLockedKeys <constr-daml-script-internal-questions-submit-error-localverdictlockedkeys-14824_>`_

    The transaction refers to locked keys which are in the process of being modified by another transaction\.

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - localVerdictLockedKeys
         - \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]
         - Locked contract keys

  .. _constr-daml-script-internal-questions-submit-error-upgradeerror-4562:

  `UpgradeError <constr-daml-script-internal-questions-submit-error-upgradeerror-4562_>`_

    Upgrade exception

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - errorType
         - `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_
         -
       * - errorMessage
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-failurestatuserror-13880:

  `FailureStatusError <constr-daml-script-internal-questions-submit-error-failurestatuserror-13880_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - failureStatus
         - `FailureStatus <https://docs.daml.com/daml/stdlib/DA-Fail.html#type-da-internal-fail-types-failurestatus-69615>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-cryptoerror-24426:

  `CryptoError <constr-daml-script-internal-questions-submit-error-cryptoerror-24426_>`_

    Crypto exceptions

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - cryptoErrorType
         - `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_
         -
       * - cryptoErrorMessage
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-deverror-73533:

  `DevError <constr-daml-script-internal-questions-submit-error-deverror-73533_>`_

    Development feature exceptions

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - devErrorType
         - `DevErrorType <type-daml-script-internal-questions-submit-error-deverrortype-71788_>`_
         -
       * - devErrorMessage
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-unknownerror-23808:

  `UnknownError <constr-daml-script-internal-questions-submit-error-unknownerror-23808_>`_

    Generic catch\-all for missing errors\.

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - unknownErrorMessage
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-truncatederror-47926:

  `TruncatedError <constr-daml-script-internal-questions-submit-error-truncatederror-47926_>`_

    One of the above error types where the full exception body did not fit into the response, and was incomplete\.
    TODO\: Should we expose this at all?

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - truncatedErrorType
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         - One of the constructor names of SubmitFailure except DevError, UnknownError, TruncatedError
       * - truncatedErrorMessage
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` :ref:`Submit <type-daml-script-internal-questions-submit-submit-31549>` \[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\]

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"actualTemplateId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"additionalDebuggingInfo\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"authorizationErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"continue\" (:ref:`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688>` a) (\[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\] \-\> a)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ AnyContractId

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractKey\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"cryptoErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"cryptoErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"devErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"devErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `DevErrorType <type-daml-script-internal-questions-submit-error-deverrortype-71788_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"duplicateContractKey\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"exc\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"expectedKey\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"expectedTemplateId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"failedTemplateKey\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"failureStatus\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `FailureStatus <https://docs.daml.com/daml/stdlib/DA-Fail.html#type-da-internal-fail-types-failurestatus-69615>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"givenKeyHash\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"globalExistingContractId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"interfaceId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"invalidTemplate\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"limit\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"localVerdictLockedContracts\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ \[AnyContractId\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"localVerdictLockedKeys\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"packageName\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"requiredInterfaceId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"requiringInterfaceId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"templateId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"truncatedErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"truncatedErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"unknownContractIds\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`NonEmpty <https://docs.daml.com/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"unknownErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"actualTemplateId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"additionalDebuggingInfo\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"authorizationErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"continue\" (:ref:`ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688>` a) (\[`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (\[:ref:`CommandResult <type-daml-script-internal-questions-commands-commandresult-15750>`\], :ref:`TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781>`)\] \-\> a)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ AnyContractId

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractKey\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"cryptoErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"cryptoErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"devErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"devErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `DevErrorType <type-daml-script-internal-questions-submit-error-deverrortype-71788_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"duplicateContractKey\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"exc\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"expectedKey\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"expectedTemplateId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"failedTemplateKey\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"failureStatus\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `FailureStatus <https://docs.daml.com/daml/stdlib/DA-Fail.html#type-da-internal-fail-types-failurestatus-69615>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"givenKeyHash\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"globalExistingContractId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"interfaceId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"invalidTemplate\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `AnyTemplate <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"limit\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"localVerdictLockedContracts\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ \[AnyContractId\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"localVerdictLockedKeys\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ \[`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"packageName\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"requiredInterfaceId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"requiringInterfaceId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"templateId\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"truncatedErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"truncatedErrorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"unknownContractIds\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (`NonEmpty <https://docs.daml.com/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"unknownErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userErrorMessage\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-upgradeerrortype-94779:

**data** `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_

  .. _constr-daml-script-internal-questions-submit-error-validationfailed-35370:

  `ValidationFailed <constr-daml-script-internal-questions-submit-error-validationfailed-35370_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - coid
         - AnyContractId
         -
       * - srcTemplateId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - dstTemplateId
         - `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - signatories
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - observers
         - \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - keyOpt
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_, \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\])
         -

  .. _constr-daml-script-internal-questions-submit-error-downgradedropdefinedfield-50092:

  `DowngradeDropDefinedField <constr-daml-script-internal-questions-submit-error-downgradedropdefinedfield-50092_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - expectedType
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - fieldIndex
         - `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-downgradefailed-38019:

  `DowngradeFailed <constr-daml-script-internal-questions-submit-error-downgradefailed-38019_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - expectedType
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"coid\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ AnyContractId

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"dstTemplateId\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"expectedType\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"fieldIndex\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"keyOpt\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_, \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"observers\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"signatories\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"srcTemplateId\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"coid\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ AnyContractId

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"dstTemplateId\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorType\" `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"expectedType\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"fieldIndex\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ `Int <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"keyOpt\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyContractKey <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_, \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"observers\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"signatories\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ \[`Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"srcTemplateId\" `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_ `TemplateTypeRep <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

Functions
---------

.. _function-daml-script-internal-questions-submit-error-isnotactive-40539:

`isNotActive <function-daml-script-internal-questions-submit-error-isnotactive-40539_>`_
  \: `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ AnyContractId

