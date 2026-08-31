.. Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-submit-error-44839:

Daml.Script.Internal.Questions.Submit.Error
===========================================

Data Types
----------

.. _type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768:

**data** `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_

  .. _constr-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-86673:

  `AuthenticationFailedUpgradeError <constr-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-86673_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - coid
         - :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`
         -
       * - srcTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - dstTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - createArg
         - `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -

  **instance** :ref:`IsUpgradeErrorType <class-daml-script-internal-questions-submit-error-isupgradeerrortype-39350>` `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"coid\" `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"createArg\" `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_ `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"dstTemplateId\" `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"srcTemplateId\" `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"coid\" `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"createArg\" `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_ `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"dstTemplateId\" `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"srcTemplateId\" `AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-submit-error-authorizationerrorsubmiterror-17511:

**data** `AuthorizationErrorSubmitError <type-daml-script-internal-questions-submit-error-authorizationerrorsubmiterror-17511_>`_

  Generic authorization failure, included missing party authority, invalid signatories, etc\.

  .. _constr-daml-script-internal-questions-submit-error-authorizationerrorsubmiterror-80828:

  `AuthorizationErrorSubmitError <constr-daml-script-internal-questions-submit-error-authorizationerrorsubmiterror-80828_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - authorizationErrorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `AuthorizationErrorSubmitError <type-daml-script-internal-questions-submit-error-authorizationerrorsubmiterror-17511_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"authorizationErrorMessage\" `AuthorizationErrorSubmitError <type-daml-script-internal-questions-submit-error-authorizationerrorsubmiterror-17511_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"authorizationErrorMessage\" `AuthorizationErrorSubmitError <type-daml-script-internal-questions-submit-error-authorizationerrorsubmiterror-17511_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-choiceguardfaileddeverror-32850:

**data** `ChoiceGuardFailedDevError <type-daml-script-internal-questions-submit-error-choiceguardfaileddeverror-32850_>`_

  .. _constr-daml-script-internal-questions-submit-error-choiceguardfaileddeverror-29681:

  `ChoiceGuardFailedDevError <constr-daml-script-internal-questions-submit-error-choiceguardfaileddeverror-29681_>`_


  **instance** :ref:`IsDevErrorType <class-daml-script-internal-questions-submit-error-isdeverrortype-77141>` `ChoiceGuardFailedDevError <type-daml-script-internal-questions-submit-error-choiceguardfaileddeverror-32850_>`_

.. _type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383:

**data** `ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383_>`_

  Attempted to use a contract as an interface that it does not implement

  .. _constr-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-51882:

  `ContractDoesNotImplementInterfaceSubmitError <constr-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-51882_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`
         -
       * - templateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - interfaceId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"interfaceId\" `ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"templateId\" `ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"interfaceId\" `ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"templateId\" `ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064:

**data** `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_

  Attempted to use a contract as a required interface that it does not implement

  .. _constr-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-21819:

  `ContractDoesNotImplementRequiringInterfaceSubmitError <constr-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-21819_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`
         -
       * - templateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - requiredInterfaceId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - requiringInterfaceId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"requiredInterfaceId\" `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"requiringInterfaceId\" `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"templateId\" `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"requiredInterfaceId\" `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"requiringInterfaceId\" `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"templateId\" `ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860:

**data** `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_

  Failed to hash a contract

  .. _constr-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-13695:

  `ContractHashingErrorSubmitError <constr-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-13695_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`
         -
       * - dstTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - createArg
         - `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -
       * - errorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"createArg\" `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_ `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"dstTemplateId\" `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorMessage\" `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"createArg\" `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_ `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"dstTemplateId\" `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorMessage\" `ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-contractidcomparabilitysubmiterror-64474:

**data** `ContractIdComparabilitySubmitError <type-daml-script-internal-questions-submit-error-contractidcomparabilitysubmiterror-64474_>`_

  Attempted to compare incomparable contract IDs\. You're doing something very wrong\.
  Two contract IDs with the same prefix are incomparable if one of them is local and the other non\-local
  or if one is relative and the other relative or absolute with a different suffix\.

  .. _constr-daml-script-internal-questions-submit-error-contractidcomparabilitysubmiterror-74619:

  `ContractIdComparabilitySubmitError <constr-daml-script-internal-questions-submit-error-contractidcomparabilitysubmiterror-74619_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - globalExistingContractId
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         - We do not know the template ID at time of comparison\.

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `ContractIdComparabilitySubmitError <type-daml-script-internal-questions-submit-error-contractidcomparabilitysubmiterror-64474_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"globalExistingContractId\" `ContractIdComparabilitySubmitError <type-daml-script-internal-questions-submit-error-contractidcomparabilitysubmiterror-64474_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"globalExistingContractId\" `ContractIdComparabilitySubmitError <type-daml-script-internal-questions-submit-error-contractidcomparabilitysubmiterror-64474_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-contractidincontractkeysubmiterror-27672:

**data** `ContractIdInContractKeySubmitError <type-daml-script-internal-questions-submit-error-contractidincontractkeysubmiterror-27672_>`_

  Illegal Contract ID found in Contract Key

  .. _constr-daml-script-internal-questions-submit-error-contractidincontractkeysubmiterror-13969:

  `ContractIdInContractKeySubmitError <constr-daml-script-internal-questions-submit-error-contractidincontractkeysubmiterror-13969_>`_


  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `ContractIdInContractKeySubmitError <type-daml-script-internal-questions-submit-error-contractidincontractkeysubmiterror-27672_>`_

.. _type-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-26927:

**data** `ContractKeyNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-26927_>`_

  Contract with given contract key could not be found

  .. _constr-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-95790:

  `ContractKeyNotFoundSubmitError <constr-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-95790_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractKey
         - :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `ContractKeyNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-26927_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractKey\" `ContractKeyNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-26927_>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractKey\" `ContractKeyNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-26927_>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`

.. _type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189:

**data** `ContractNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189_>`_

  Contract with given contract ID could not be found, and has never existed on this participant
  When run on Canton, there may be more than one contract ID, and additionalDebuggingInfo is always None
  On the other hand, when run on IDELedger, there is only ever one contract ID, and additionalDebuggingInfo is always Some

  .. _constr-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-9714:

  `ContractNotFoundSubmitError <constr-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-9714_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - unknownContractIds
         - `NonEmpty <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         - Provided as text, as we do not know the template ID of a contract if the lookup fails
       * - additionalDebuggingInfo
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199>`
         - should always be None in Canton's case, see https\://github\.com/digital\-asset/daml/issues/17154

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `ContractNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"additionalDebuggingInfo\" `ContractNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199>`)

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"unknownContractIds\" `ContractNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189_>`_ (`NonEmpty <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"additionalDebuggingInfo\" `ContractNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199>`)

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"unknownContractIds\" `ContractNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189_>`_ (`NonEmpty <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

.. _type-daml-script-internal-questions-submit-error-createemptycontractkeymaintainerssubmiterror-51894:

**data** `CreateEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-createemptycontractkeymaintainerssubmiterror-51894_>`_

  Attempted to create a contract with empty contract key maintainers

  .. _constr-daml-script-internal-questions-submit-error-createemptycontractkeymaintainerssubmiterror-12891:

  `CreateEmptyContractKeyMaintainersSubmitError <constr-daml-script-internal-questions-submit-error-createemptycontractkeymaintainerssubmiterror-12891_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - invalidTemplate
         - `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `CreateEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-createemptycontractkeymaintainerssubmiterror-51894_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"invalidTemplate\" `CreateEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-createemptycontractkeymaintainerssubmiterror-51894_>`_ `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"invalidTemplate\" `CreateEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-createemptycontractkeymaintainerssubmiterror-51894_>`_ `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

.. _type-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-70296:

**data** `CryptoErrorSubmitError <type-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-70296_>`_

  Crypto exceptions

  .. _constr-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-52709:

  `CryptoErrorSubmitError <constr-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-52709_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - cryptoErrorType
         - :ref:`AnyCryptoErrorType <type-daml-script-internal-questions-submit-error-stable-anycryptoerrortype-anycryptoerrortype-64150>`
         -
       * - cryptoErrorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `CryptoErrorSubmitError <type-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-70296_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"cryptoErrorMessage\" `CryptoErrorSubmitError <type-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-70296_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"cryptoErrorType\" `CryptoErrorSubmitError <type-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-70296_>`_ :ref:`AnyCryptoErrorType <type-daml-script-internal-questions-submit-error-stable-anycryptoerrortype-anycryptoerrortype-64150>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"cryptoErrorMessage\" `CryptoErrorSubmitError <type-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-70296_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"cryptoErrorType\" `CryptoErrorSubmitError <type-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-70296_>`_ :ref:`AnyCryptoErrorType <type-daml-script-internal-questions-submit-error-stable-anycryptoerrortype-anycryptoerrortype-64150>`

.. _type-daml-script-internal-questions-submit-error-deverrorsubmiterror-79959:

**data** `DevErrorSubmitError <type-daml-script-internal-questions-submit-error-deverrorsubmiterror-79959_>`_

  Development feature exceptions

  .. _constr-daml-script-internal-questions-submit-error-deverrorsubmiterror-76132:

  `DevErrorSubmitError <constr-daml-script-internal-questions-submit-error-deverrorsubmiterror-76132_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - devErrorType
         - :ref:`AnyDevErrorType <type-daml-script-internal-questions-submit-error-stable-anydeverrortype-anydeverrortype-93864>`
         -
       * - devErrorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `DevErrorSubmitError <type-daml-script-internal-questions-submit-error-deverrorsubmiterror-79959_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"devErrorMessage\" `DevErrorSubmitError <type-daml-script-internal-questions-submit-error-deverrorsubmiterror-79959_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"devErrorType\" `DevErrorSubmitError <type-daml-script-internal-questions-submit-error-deverrorsubmiterror-79959_>`_ :ref:`AnyDevErrorType <type-daml-script-internal-questions-submit-error-stable-anydeverrortype-anydeverrortype-93864>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"devErrorMessage\" `DevErrorSubmitError <type-daml-script-internal-questions-submit-error-deverrorsubmiterror-79959_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"devErrorType\" `DevErrorSubmitError <type-daml-script-internal-questions-submit-error-deverrorsubmiterror-79959_>`_ :ref:`AnyDevErrorType <type-daml-script-internal-questions-submit-error-stable-anydeverrortype-anydeverrortype-93864>`

.. _type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935:

**data** `DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935_>`_

  Given disclosed contract key does not match the contract key of the contract on ledger\.

  .. _constr-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-72748:

  `DisclosedContractKeyHashingErrorSubmitError <constr-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-72748_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`
         -
       * - expectedKey
         - :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`
         -
       * - givenKeyHash
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"expectedKey\" `DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935_>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"givenKeyHash\" `DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"expectedKey\" `DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935_>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"givenKeyHash\" `DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-30134:

**data** `DuplicateContractKeySubmitError <type-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-30134_>`_

  Attempted to create a contract with a contract key that already exists

  .. _constr-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-67517:

  `DuplicateContractKeySubmitError <constr-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-67517_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - duplicateContractKey
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`
         - Canton will often not provide this key, IDELedger will

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `DuplicateContractKeySubmitError <type-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-30134_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"duplicateContractKey\" `DuplicateContractKeySubmitError <type-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-30134_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`)

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"duplicateContractKey\" `DuplicateContractKeySubmitError <type-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-30134_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`)

.. _type-daml-script-internal-questions-submit-error-effectfulrollbackerrorsubmiterror-25900:

**data** `EffectfulRollbackErrorSubmitError <type-daml-script-internal-questions-submit-error-effectfulrollbackerrorsubmiterror-25900_>`_

  Rollback exceptions

  .. _constr-daml-script-internal-questions-submit-error-effectfulrollbackerrorsubmiterror-66691:

  `EffectfulRollbackErrorSubmitError <constr-daml-script-internal-questions-submit-error-effectfulrollbackerrorsubmiterror-66691_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - effectfulRollbackErrorMsg
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `EffectfulRollbackErrorSubmitError <type-daml-script-internal-questions-submit-error-effectfulrollbackerrorsubmiterror-25900_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"effectfulRollbackErrorMsg\" `EffectfulRollbackErrorSubmitError <type-daml-script-internal-questions-submit-error-effectfulrollbackerrorsubmiterror-25900_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"effectfulRollbackErrorMsg\" `EffectfulRollbackErrorSubmitError <type-daml-script-internal-questions-submit-error-effectfulrollbackerrorsubmiterror-25900_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-executionfailedexternalcallerror-27747:

**data** `ExecutionFailedExternalCallError <type-daml-script-internal-questions-submit-error-executionfailedexternalcallerror-27747_>`_

  .. _constr-daml-script-internal-questions-submit-error-executionfailedexternalcallerror-37214:

  `ExecutionFailedExternalCallError <constr-daml-script-internal-questions-submit-error-executionfailedexternalcallerror-37214_>`_


  **instance** :ref:`IsExternalCallErrorType <class-daml-script-internal-questions-submit-error-isexternalcallerrortype-49854>` `ExecutionFailedExternalCallError <type-daml-script-internal-questions-submit-error-executionfailedexternalcallerror-27747_>`_

.. _type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640:

**data** `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_

  External\-call interpretation exception

  .. _constr-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-46121:

  `ExternalCallErrorSubmitError <constr-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-46121_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - externalCallErrorType
         - :ref:`AnyExternalCallErrorType <type-daml-script-internal-questions-submit-error-stable-anyexternalcallerrortype-anyexternalcallerrortype-11122>`
         -
       * - extensionId
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - functionId
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - externalCallErrorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"extensionId\" `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"externalCallErrorMessage\" `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"externalCallErrorType\" `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_ :ref:`AnyExternalCallErrorType <type-daml-script-internal-questions-submit-error-stable-anyexternalcallerrortype-anyexternalcallerrortype-11122>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"functionId\" `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"extensionId\" `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"externalCallErrorMessage\" `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"externalCallErrorType\" `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_ :ref:`AnyExternalCallErrorType <type-daml-script-internal-questions-submit-error-stable-anyexternalcallerrortype-anyexternalcallerrortype-11122>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"functionId\" `ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-failurestatuserrorsubmiterror-57372:

**data** `FailureStatusErrorSubmitError <type-daml-script-internal-questions-submit-error-failurestatuserrorsubmiterror-57372_>`_

  Exception resulting from call to ``failWithStatus``

  .. _constr-daml-script-internal-questions-submit-error-failurestatuserrorsubmiterror-86427:

  `FailureStatusErrorSubmitError <constr-daml-script-internal-questions-submit-error-failurestatuserrorsubmiterror-86427_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - failureStatus
         - `FailureStatus <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Fail.html#type-da-internal-fail-types-failurestatus-69615>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `FailureStatusErrorSubmitError <type-daml-script-internal-questions-submit-error-failurestatuserrorsubmiterror-57372_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"failureStatus\" `FailureStatusErrorSubmitError <type-daml-script-internal-questions-submit-error-failurestatuserrorsubmiterror-57372_>`_ `FailureStatus <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Fail.html#type-da-internal-fail-types-failurestatus-69615>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"failureStatus\" `FailureStatusErrorSubmitError <type-daml-script-internal-questions-submit-error-failurestatuserrorsubmiterror-57372_>`_ `FailureStatus <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Fail.html#type-da-internal-fail-types-failurestatus-69615>`_

.. _type-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-95321:

**data** `FetchEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-95321_>`_

  Attempted to fetch a contract with empty contract key maintainers

  .. _constr-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-26230:

  `FetchEmptyContractKeyMaintainersSubmitError <constr-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-26230_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - failedTemplateKey
         - :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `FetchEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-95321_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"failedTemplateKey\" `FetchEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-95321_>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"failedTemplateKey\" `FetchEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-95321_>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`

.. _type-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-13545:

**data** `InconsistentContractKeySubmitError <type-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-13545_>`_

  Contract key lookup yielded different results

  .. _constr-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-86872:

  `InconsistentContractKeySubmitError <constr-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-86872_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractKey
         - :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `InconsistentContractKeySubmitError <type-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-13545_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractKey\" `InconsistentContractKeySubmitError <type-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-13545_>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractKey\" `InconsistentContractKeySubmitError <type-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-13545_>`_ :ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`

.. _type-daml-script-internal-questions-submit-error-invalidoutputexternalcallerror-81292:

**data** `InvalidOutputExternalCallError <type-daml-script-internal-questions-submit-error-invalidoutputexternalcallerror-81292_>`_

  .. _constr-daml-script-internal-questions-submit-error-invalidoutputexternalcallerror-8201:

  `InvalidOutputExternalCallError <constr-daml-script-internal-questions-submit-error-invalidoutputexternalcallerror-8201_>`_


  **instance** :ref:`IsExternalCallErrorType <class-daml-script-internal-questions-submit-error-isexternalcallerrortype-49854>` `InvalidOutputExternalCallError <type-daml-script-internal-questions-submit-error-invalidoutputexternalcallerror-81292_>`_

.. _type-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-33196:

**data** `LocalVerdictLockedContractsSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-33196_>`_

  The transaction refers to locked contracts which are in the process of being created, transferred, or
  archived by another transaction\. If the other transaction fails, this transaction could be successfully retried\.

  .. _constr-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-22537:

  `LocalVerdictLockedContractsSubmitError <constr-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-22537_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - localVerdictLockedContracts
         - \[:ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`\]
         - Locked contract ids

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `LocalVerdictLockedContractsSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-33196_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"localVerdictLockedContracts\" `LocalVerdictLockedContractsSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-33196_>`_ \[:ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"localVerdictLockedContracts\" `LocalVerdictLockedContractsSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-33196_>`_ \[:ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`\]

.. _type-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-25684:

**data** `LocalVerdictLockedKeysSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-25684_>`_

  The transaction refers to locked keys which are in the process of being modified by another transaction\.

  .. _constr-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-41259:

  `LocalVerdictLockedKeysSubmitError <constr-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-41259_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - localVerdictLockedKeys
         - \[:ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`\]
         - Locked contract keys

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `LocalVerdictLockedKeysSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-25684_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"localVerdictLockedKeys\" `LocalVerdictLockedKeysSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-25684_>`_ \[:ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"localVerdictLockedKeys\" `LocalVerdictLockedKeysSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-25684_>`_ \[:ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`\]

.. _type-daml-script-internal-questions-submit-error-malformedbyteencodingcryptoerror-55788:

**data** `MalformedByteEncodingCryptoError <type-daml-script-internal-questions-submit-error-malformedbyteencodingcryptoerror-55788_>`_

  .. _constr-daml-script-internal-questions-submit-error-malformedbyteencodingcryptoerror-58001:

  `MalformedByteEncodingCryptoError <constr-daml-script-internal-questions-submit-error-malformedbyteencodingcryptoerror-58001_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - value
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsCryptoErrorType <class-daml-script-internal-questions-submit-error-iscryptoerrortype-84910>` `MalformedByteEncodingCryptoError <type-daml-script-internal-questions-submit-error-malformedbyteencodingcryptoerror-55788_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"value\" `MalformedByteEncodingCryptoError <type-daml-script-internal-questions-submit-error-malformedbyteencodingcryptoerror-55788_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"value\" `MalformedByteEncodingCryptoError <type-daml-script-internal-questions-submit-error-malformedbyteencodingcryptoerror-55788_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-malformedkeycryptoerror-42901:

**data** `MalformedKeyCryptoError <type-daml-script-internal-questions-submit-error-malformedkeycryptoerror-42901_>`_

  .. _constr-daml-script-internal-questions-submit-error-malformedkeycryptoerror-90034:

  `MalformedKeyCryptoError <constr-daml-script-internal-questions-submit-error-malformedkeycryptoerror-90034_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - keyValue
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsCryptoErrorType <class-daml-script-internal-questions-submit-error-iscryptoerrortype-84910>` `MalformedKeyCryptoError <type-daml-script-internal-questions-submit-error-malformedkeycryptoerror-42901_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"keyValue\" `MalformedKeyCryptoError <type-daml-script-internal-questions-submit-error-malformedkeycryptoerror-42901_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"keyValue\" `MalformedKeyCryptoError <type-daml-script-internal-questions-submit-error-malformedkeycryptoerror-42901_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-malformedsignaturecryptoerror-90694:

**data** `MalformedSignatureCryptoError <type-daml-script-internal-questions-submit-error-malformedsignaturecryptoerror-90694_>`_

  .. _constr-daml-script-internal-questions-submit-error-malformedsignaturecryptoerror-17025:

  `MalformedSignatureCryptoError <constr-daml-script-internal-questions-submit-error-malformedsignaturecryptoerror-17025_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - signatureValue
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsCryptoErrorType <class-daml-script-internal-questions-submit-error-iscryptoerrortype-84910>` `MalformedSignatureCryptoError <type-daml-script-internal-questions-submit-error-malformedsignaturecryptoerror-90694_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"signatureValue\" `MalformedSignatureCryptoError <type-daml-script-internal-questions-submit-error-malformedsignaturecryptoerror-90694_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"signatureValue\" `MalformedSignatureCryptoError <type-daml-script-internal-questions-submit-error-malformedsignaturecryptoerror-90694_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-noncomparablevaluessubmiterror-29684:

**data** `NonComparableValuesSubmitError <type-daml-script-internal-questions-submit-error-noncomparablevaluessubmiterror-29684_>`_

  Attempted to compare values that are not comparable

  .. _constr-daml-script-internal-questions-submit-error-noncomparablevaluessubmiterror-29469:

  `NonComparableValuesSubmitError <constr-daml-script-internal-questions-submit-error-noncomparablevaluessubmiterror-29469_>`_


  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `NonComparableValuesSubmitError <type-daml-script-internal-questions-submit-error-noncomparablevaluessubmiterror-29684_>`_

.. _type-daml-script-internal-questions-submit-error-preparationfailedexternalcallerror-68134:

**data** `PreparationFailedExternalCallError <type-daml-script-internal-questions-submit-error-preparationfailedexternalcallerror-68134_>`_

  .. _constr-daml-script-internal-questions-submit-error-preparationfailedexternalcallerror-78943:

  `PreparationFailedExternalCallError <constr-daml-script-internal-questions-submit-error-preparationfailedexternalcallerror-78943_>`_


  **instance** :ref:`IsExternalCallErrorType <class-daml-script-internal-questions-submit-error-isexternalcallerrortype-49854>` `PreparationFailedExternalCallError <type-daml-script-internal-questions-submit-error-preparationfailedexternalcallerror-68134_>`_

.. _type-daml-script-internal-questions-submit-error-templatepreconditionviolatedsubmiterror-80122:

**data** `TemplatePreconditionViolatedSubmitError <type-daml-script-internal-questions-submit-error-templatepreconditionviolatedsubmiterror-80122_>`_

  Failure due to false result from ``ensure``, strictly pre\-exception\.
  According to docs, not throwable with LF \>\= 1\.14\.
  On LF \>\= 1\.14, a failed ``ensure`` will result in a ``PreconditionFailed``
  exception wrapped in ``UnhandledException``\.

  .. _constr-daml-script-internal-questions-submit-error-templatepreconditionviolatedsubmiterror-43049:

  `TemplatePreconditionViolatedSubmitError <constr-daml-script-internal-questions-submit-error-templatepreconditionviolatedsubmiterror-43049_>`_


  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `TemplatePreconditionViolatedSubmitError <type-daml-script-internal-questions-submit-error-templatepreconditionviolatedsubmiterror-80122_>`_

.. _type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244:

**data** `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_

  .. _constr-daml-script-internal-questions-submit-error-translationfailedupgradeerror-94095:

  `TranslationFailedUpgradeError <constr-daml-script-internal-questions-submit-error-translationfailedupgradeerror-94095_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - mCoid
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`
         -
       * - srcTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - dstTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - createArg
         - `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -

  **instance** :ref:`IsUpgradeErrorType <class-daml-script-internal-questions-submit-error-isupgradeerrortype-39350>` `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"createArg\" `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_ `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"dstTemplateId\" `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"mCoid\" `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`)

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"srcTemplateId\" `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"createArg\" `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_ `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"dstTemplateId\" `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"mCoid\" `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`)

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"srcTemplateId\" `TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-96038:

**data** `TruncatedErrorSubmitError <type-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-96038_>`_

  One of the above error types where the full exception body did not fit into the response, and was incomplete\.

  .. _constr-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-53465:

  `TruncatedErrorSubmitError <constr-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-53465_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - truncatedErrorType
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         - One of the constructor names of SubmitFailure except DevError, UnknownError, TruncatedError
       * - truncatedErrorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `TruncatedErrorSubmitError <type-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-96038_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"truncatedErrorMessage\" `TruncatedErrorSubmitError <type-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-96038_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"truncatedErrorType\" `TruncatedErrorSubmitError <type-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-96038_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"truncatedErrorMessage\" `TruncatedErrorSubmitError <type-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-96038_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"truncatedErrorType\" `TruncatedErrorSubmitError <type-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-96038_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-unhandledexceptionsubmiterror-90486:

**data** `UnhandledExceptionSubmitError <type-daml-script-internal-questions-submit-error-unhandledexceptionsubmiterror-90486_>`_

  Unhandled user thrown exception

  .. _constr-daml-script-internal-questions-submit-error-unhandledexceptionsubmiterror-66645:

  `UnhandledExceptionSubmitError <constr-daml-script-internal-questions-submit-error-unhandledexceptionsubmiterror-66645_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - exc
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyException <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_
         - Errors more complex than simple records cannot currently be encoded over the grpc status\. Such errors will be missing here\.

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `UnhandledExceptionSubmitError <type-daml-script-internal-questions-submit-error-unhandledexceptionsubmiterror-90486_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"exc\" `UnhandledExceptionSubmitError <type-daml-script-internal-questions-submit-error-unhandledexceptionsubmiterror-90486_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyException <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_)

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"exc\" `UnhandledExceptionSubmitError <type-daml-script-internal-questions-submit-error-unhandledexceptionsubmiterror-90486_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyException <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_)

.. _type-daml-script-internal-questions-submit-error-unknownerrorsubmiterror-20400:

**data** `UnknownErrorSubmitError <type-daml-script-internal-questions-submit-error-unknownerrorsubmiterror-20400_>`_

  Generic catch\-all for missing errors\.

  .. _constr-daml-script-internal-questions-submit-error-unknownerrorsubmiterror-36583:

  `UnknownErrorSubmitError <constr-daml-script-internal-questions-submit-error-unknownerrorsubmiterror-36583_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - unknownErrorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `UnknownErrorSubmitError <type-daml-script-internal-questions-submit-error-unknownerrorsubmiterror-20400_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"unknownErrorMessage\" `UnknownErrorSubmitError <type-daml-script-internal-questions-submit-error-unknownerrorsubmiterror-20400_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"unknownErrorMessage\" `UnknownErrorSubmitError <type-daml-script-internal-questions-submit-error-unknownerrorsubmiterror-20400_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-unknownnewfeaturedeverror-77479:

**data** `UnknownNewFeatureDevError <type-daml-script-internal-questions-submit-error-unknownnewfeaturedeverror-77479_>`_

  This should never happen \- Update Scripts when you see this!

  .. _constr-daml-script-internal-questions-submit-error-unknownnewfeaturedeverror-15260:

  `UnknownNewFeatureDevError <constr-daml-script-internal-questions-submit-error-unknownnewfeaturedeverror-15260_>`_


  **instance** :ref:`IsDevErrorType <class-daml-script-internal-questions-submit-error-isdeverrortype-77141>` `UnknownNewFeatureDevError <type-daml-script-internal-questions-submit-error-unknownnewfeaturedeverror-77479_>`_

.. _type-daml-script-internal-questions-submit-error-unresolvedpackagenamesubmiterror-60769:

**data** `UnresolvedPackageNameSubmitError <type-daml-script-internal-questions-submit-error-unresolvedpackagenamesubmiterror-60769_>`_

  No vetted package with given package name could be found

  .. _constr-daml-script-internal-questions-submit-error-unresolvedpackagenamesubmiterror-52484:

  `UnresolvedPackageNameSubmitError <constr-daml-script-internal-questions-submit-error-unresolvedpackagenamesubmiterror-52484_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - packageName
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `UnresolvedPackageNameSubmitError <type-daml-script-internal-questions-submit-error-unresolvedpackagenamesubmiterror-60769_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"packageName\" `UnresolvedPackageNameSubmitError <type-daml-script-internal-questions-submit-error-unresolvedpackagenamesubmiterror-60769_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"packageName\" `UnresolvedPackageNameSubmitError <type-daml-script-internal-questions-submit-error-unresolvedpackagenamesubmiterror-60769_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-unsupportedcontractidsubmiterror-39223:

**data** `UnsupportedContractIdSubmitError <type-daml-script-internal-questions-submit-error-unsupportedcontractidsubmiterror-39223_>`_

  Unsupported contract id type/version

  .. _constr-daml-script-internal-questions-submit-error-unsupportedcontractidsubmiterror-31038:

  `UnsupportedContractIdSubmitError <constr-daml-script-internal-questions-submit-error-unsupportedcontractidsubmiterror-31038_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - unknownContractId
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `UnsupportedContractIdSubmitError <type-daml-script-internal-questions-submit-error-unsupportedcontractidsubmiterror-39223_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"unknownContractId\" `UnsupportedContractIdSubmitError <type-daml-script-internal-questions-submit-error-unsupportedcontractidsubmiterror-39223_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"unknownContractId\" `UnsupportedContractIdSubmitError <type-daml-script-internal-questions-submit-error-unsupportedcontractidsubmiterror-39223_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-51646:

**data** `UpgradeErrorSubmitError <type-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-51646_>`_

  Upgrade exception

  .. _constr-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-93753:

  `UpgradeErrorSubmitError <constr-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-93753_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - errorType
         - :ref:`AnyUpgradeErrorType <type-daml-script-internal-questions-submit-error-stable-anyupgradeerrortype-anyupgradeerrortype-9932>`
         -
       * - errorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `UpgradeErrorSubmitError <type-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-51646_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorMessage\" `UpgradeErrorSubmitError <type-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-51646_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"errorType\" `UpgradeErrorSubmitError <type-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-51646_>`_ :ref:`AnyUpgradeErrorType <type-daml-script-internal-questions-submit-error-stable-anyupgradeerrortype-anyupgradeerrortype-9932>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorMessage\" `UpgradeErrorSubmitError <type-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-51646_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"errorType\" `UpgradeErrorSubmitError <type-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-51646_>`_ :ref:`AnyUpgradeErrorType <type-daml-script-internal-questions-submit-error-stable-anyupgradeerrortype-anyupgradeerrortype-9932>`

.. _type-daml-script-internal-questions-submit-error-usererrorsubmiterror-77592:

**data** `UserErrorSubmitError <type-daml-script-internal-questions-submit-error-usererrorsubmiterror-77592_>`_

  Transaction failure due to abort/assert calls pre\-exceptions

  .. _constr-daml-script-internal-questions-submit-error-usererrorsubmiterror-61125:

  `UserErrorSubmitError <constr-daml-script-internal-questions-submit-error-usererrorsubmiterror-61125_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userErrorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `UserErrorSubmitError <type-daml-script-internal-questions-submit-error-usererrorsubmiterror-77592_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"userErrorMessage\" `UserErrorSubmitError <type-daml-script-internal-questions-submit-error-usererrorsubmiterror-77592_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"userErrorMessage\" `UserErrorSubmitError <type-daml-script-internal-questions-submit-error-usererrorsubmiterror-77592_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297:

**data** `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_

  .. _constr-daml-script-internal-questions-submit-error-validationfailedupgradeerror-65884:

  `ValidationFailedUpgradeError <constr-daml-script-internal-questions-submit-error-validationfailedupgradeerror-65884_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - coid
         - :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`
         -
       * - srcTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - dstTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - srcPackageName
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - dstPackageName
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - originalSignatories
         - \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - originalNonSignatoryStakeholders
         - \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - originalKeyOpt
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (:ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\])
         -
       * - recomputedSignatories
         - \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - recomputedNonSignatoryStakeholders
         - \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - recomputedKeyOpt
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (:ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\])
         -

  **instance** :ref:`IsUpgradeErrorType <class-daml-script-internal-questions-submit-error-isupgradeerrortype-39350>` `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"coid\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"dstPackageName\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"dstTemplateId\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"originalKeyOpt\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (:ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"originalNonSignatoryStakeholders\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"originalSignatories\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"recomputedKeyOpt\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (:ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"recomputedNonSignatoryStakeholders\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"recomputedSignatories\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"srcPackageName\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"srcTemplateId\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"coid\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"dstPackageName\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"dstTemplateId\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"originalKeyOpt\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (:ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"originalNonSignatoryStakeholders\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"originalSignatories\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"recomputedKeyOpt\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (:ref:`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404>`, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"recomputedNonSignatoryStakeholders\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"recomputedSignatories\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"srcPackageName\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"srcTemplateId\" `ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

.. _type-daml-script-internal-questions-submit-error-valuenestingsubmiterror-39889:

**data** `ValueNestingSubmitError <type-daml-script-internal-questions-submit-error-valuenestingsubmiterror-39889_>`_

  A value has been nested beyond a given depth limit

  .. _constr-daml-script-internal-questions-submit-error-valuenestingsubmiterror-72406:

  `ValueNestingSubmitError <constr-daml-script-internal-questions-submit-error-valuenestingsubmiterror-72406_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - limit
         - `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         - Nesting limit that was exceeded

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `ValueNestingSubmitError <type-daml-script-internal-questions-submit-error-valuenestingsubmiterror-39889_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"limit\" `ValueNestingSubmitError <type-daml-script-internal-questions-submit-error-valuenestingsubmiterror-39889_>`_ `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"limit\" `ValueNestingSubmitError <type-daml-script-internal-questions-submit-error-valuenestingsubmiterror-39889_>`_ `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_

.. _type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056:

**data** `WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056_>`_

  Attempted to exercise/fetch a contract with the wrong template type

  .. _constr-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-26471:

  `WronglyTypedContractSubmitError <constr-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-26471_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`
         - Any contract Id of the actual contract
       * - expectedTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - actualTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -

  **instance** :ref:`IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591>` `WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"actualTemplateId\" `WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" `WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"expectedTemplateId\" `WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"actualTemplateId\" `WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" `WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"expectedTemplateId\" `WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056_>`_ `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_

