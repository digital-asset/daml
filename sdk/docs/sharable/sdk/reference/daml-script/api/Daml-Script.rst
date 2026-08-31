.. Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-55737:

Daml.Script
===========

The Daml Script testing library\.

Typeclasses
-----------

.. _class-daml-script-internal-questions-submit-issubmitoptions-64211:

**class** `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options **where**

  Defines a type that can be transformed into a SubmitOptions

  .. _function-daml-script-internal-questions-submit-tosubmitoptions-99319:

  `toSubmitOptions <function-daml-script-internal-questions-submit-tosubmitoptions-99319_>`_
    \: options \-\> `SubmitOptions <type-daml-script-internal-questions-submit-stable-submitoptions-submitoptions-27150_>`_

.. _class-daml-script-internal-questions-submit-scriptsubmit-55101:

**class** `Applicative <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257>`_ script \=\> `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script **where**

  Defines an applicative that can run transaction submissions\. Usually this is simply ``Script``\.

  .. _function-daml-script-internal-questions-submit-liftsubmission-99954:

  `liftSubmission <function-daml-script-internal-questions-submit-liftsubmission-99954_>`_
    \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a \-\> script a

.. _class-daml-script-internal-questions-submit-error-issubmiterror-52591:

**class** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ e **where**

  Allows casting AnySubmitError into and out of specific errors\.

  .. _function-daml-script-internal-questions-submit-error-toanysubmiterror-93306:

  `toAnySubmitError <function-daml-script-internal-questions-submit-error-toanysubmiterror-93306_>`_
    \: e \-\> :ref:`AnySubmitError <type-daml-script-internal-questions-submit-error-stable-anysubmiterror-anysubmiterror-96036>`

    Transform a submit error into an AnySubmitError

  .. _function-daml-script-internal-questions-submit-error-fromanysubmiterror-37567:

  `fromAnySubmitError <function-daml-script-internal-questions-submit-error-fromanysubmiterror-37567_>`_
    \: :ref:`AnySubmitError <type-daml-script-internal-questions-submit-error-stable-anysubmiterror-anysubmiterror-96036>` \-\> `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ e

    Attempt to transform an AnySubmitError into a specific submit error, gives ``None`` if the underlying type of the AnySubmitError
    does not match\.
    This can be used with type application, i\.e\. ``fromAnySubmitError @UnsupportedContractIdSubmitError``
    or forced with view patterns\:

    .. code-block:: daml

        case anySubmitError of
          (fromAnySubmitError -> UnsupportedContractIdSubmitError {..}) -> ...

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`AuthorizationErrorSubmitError <type-daml-script-internal-questions-submit-error-authorizationerrorsubmiterror-17511>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`ContractIdComparabilitySubmitError <type-daml-script-internal-questions-submit-error-contractidcomparabilitysubmiterror-64474>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`ContractIdInContractKeySubmitError <type-daml-script-internal-questions-submit-error-contractidincontractkeysubmiterror-27672>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`ContractKeyNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-26927>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`ContractNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`CreateEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-createemptycontractkeymaintainerssubmiterror-51894>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`CryptoErrorSubmitError <type-daml-script-internal-questions-submit-error-cryptoerrorsubmiterror-70296>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`DevErrorSubmitError <type-daml-script-internal-questions-submit-error-deverrorsubmiterror-79959>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`DuplicateContractKeySubmitError <type-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-30134>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`EffectfulRollbackErrorSubmitError <type-daml-script-internal-questions-submit-error-effectfulrollbackerrorsubmiterror-25900>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`ExternalCallErrorSubmitError <type-daml-script-internal-questions-submit-error-externalcallerrorsubmiterror-57640>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`FailureStatusErrorSubmitError <type-daml-script-internal-questions-submit-error-failurestatuserrorsubmiterror-57372>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`FetchEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-95321>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`InconsistentContractKeySubmitError <type-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-13545>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`LocalVerdictLockedContractsSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-33196>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`LocalVerdictLockedKeysSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-25684>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`NonComparableValuesSubmitError <type-daml-script-internal-questions-submit-error-noncomparablevaluessubmiterror-29684>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`TemplatePreconditionViolatedSubmitError <type-daml-script-internal-questions-submit-error-templatepreconditionviolatedsubmiterror-80122>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`TruncatedErrorSubmitError <type-daml-script-internal-questions-submit-error-truncatederrorsubmiterror-96038>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`UnhandledExceptionSubmitError <type-daml-script-internal-questions-submit-error-unhandledexceptionsubmiterror-90486>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`UnknownErrorSubmitError <type-daml-script-internal-questions-submit-error-unknownerrorsubmiterror-20400>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`UnresolvedPackageNameSubmitError <type-daml-script-internal-questions-submit-error-unresolvedpackagenamesubmiterror-60769>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`UnsupportedContractIdSubmitError <type-daml-script-internal-questions-submit-error-unsupportedcontractidsubmiterror-39223>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`UpgradeErrorSubmitError <type-daml-script-internal-questions-submit-error-upgradeerrorsubmiterror-51646>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`UserErrorSubmitError <type-daml-script-internal-questions-submit-error-usererrorsubmiterror-77592>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`ValueNestingSubmitError <type-daml-script-internal-questions-submit-error-valuenestingsubmiterror-39889>`

  **instance** `IsSubmitError <class-daml-script-internal-questions-submit-error-issubmiterror-52591_>`_ :ref:`WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056>`

.. _class-daml-script-internal-questions-submit-error-isupgradeerrortype-39350:

**class** `IsUpgradeErrorType <class-daml-script-internal-questions-submit-error-isupgradeerrortype-39350_>`_ e **where**

  SCU related submission errors
  Allows casting AnyUpgradeErrorType into and out of specific upgrade error types\.

  .. _function-daml-script-internal-questions-submit-error-toanyupgradeerrortype-77005:

  `toAnyUpgradeErrorType <function-daml-script-internal-questions-submit-error-toanyupgradeerrortype-77005_>`_
    \: e \-\> :ref:`AnyUpgradeErrorType <type-daml-script-internal-questions-submit-error-stable-anyupgradeerrortype-anyupgradeerrortype-9932>`

    Transform an upgrade error type into an AnyUpgradeErrorType

  .. _function-daml-script-internal-questions-submit-error-fromanyupgradeerrortype-11058:

  `fromAnyUpgradeErrorType <function-daml-script-internal-questions-submit-error-fromanyupgradeerrortype-11058_>`_
    \: :ref:`AnyUpgradeErrorType <type-daml-script-internal-questions-submit-error-stable-anyupgradeerrortype-anyupgradeerrortype-9932>` \-\> `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ e

    Attempt to transform an AnyUpgradeErrorType into a specific upgrade error type, gives ``None`` if the underlying type of the AnyUpgradeErrorType
    does not match\.

  **instance** `IsUpgradeErrorType <class-daml-script-internal-questions-submit-error-isupgradeerrortype-39350_>`_ :ref:`AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768>`

  **instance** `IsUpgradeErrorType <class-daml-script-internal-questions-submit-error-isupgradeerrortype-39350_>`_ :ref:`TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244>`

  **instance** `IsUpgradeErrorType <class-daml-script-internal-questions-submit-error-isupgradeerrortype-39350_>`_ :ref:`ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297>`

.. _class-daml-script-internal-questions-submit-error-iscryptoerrortype-84910:

**class** `IsCryptoErrorType <class-daml-script-internal-questions-submit-error-iscryptoerrortype-84910_>`_ e **where**

  Daml Crypto (Secp256k1) related submission errors
  Allows casting AnyCryptoErrorType into and out of specific crypto error types\.

  .. _function-daml-script-internal-questions-submit-error-toanycryptoerrortype-48571:

  `toAnyCryptoErrorType <function-daml-script-internal-questions-submit-error-toanycryptoerrortype-48571_>`_
    \: e \-\> :ref:`AnyCryptoErrorType <type-daml-script-internal-questions-submit-error-stable-anycryptoerrortype-anycryptoerrortype-64150>`

    Transform a crypto error type into an AnyCryptoErrorType

  .. _function-daml-script-internal-questions-submit-error-fromanycryptoerrortype-85634:

  `fromAnyCryptoErrorType <function-daml-script-internal-questions-submit-error-fromanycryptoerrortype-85634_>`_
    \: :ref:`AnyCryptoErrorType <type-daml-script-internal-questions-submit-error-stable-anycryptoerrortype-anycryptoerrortype-64150>` \-\> `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ e

    Attempt to transform an AnyCryptoErrorType into a specific crypto error type, gives ``None`` if the underlying type of the AnyCryptoErrorType
    does not match\.

  **instance** `IsCryptoErrorType <class-daml-script-internal-questions-submit-error-iscryptoerrortype-84910_>`_ :ref:`MalformedByteEncodingCryptoError <type-daml-script-internal-questions-submit-error-malformedbyteencodingcryptoerror-55788>`

  **instance** `IsCryptoErrorType <class-daml-script-internal-questions-submit-error-iscryptoerrortype-84910_>`_ :ref:`MalformedKeyCryptoError <type-daml-script-internal-questions-submit-error-malformedkeycryptoerror-42901>`

  **instance** `IsCryptoErrorType <class-daml-script-internal-questions-submit-error-iscryptoerrortype-84910_>`_ :ref:`MalformedSignatureCryptoError <type-daml-script-internal-questions-submit-error-malformedsignaturecryptoerror-90694>`

.. _class-daml-script-internal-questions-submit-error-isexternalcallerrortype-49854:

**class** `IsExternalCallErrorType <class-daml-script-internal-questions-submit-error-isexternalcallerrortype-49854_>`_ e **where**

  External\-call related submission errors, one per stage\: preparing the
  call, executing it, and validating the service output\.
  Allows casting AnyExternalCallErrorType into and out of specific external\-call error types\.

  .. _function-daml-script-internal-questions-submit-error-toanyexternalcallerrortype-29263:

  `toAnyExternalCallErrorType <function-daml-script-internal-questions-submit-error-toanyexternalcallerrortype-29263_>`_
    \: e \-\> :ref:`AnyExternalCallErrorType <type-daml-script-internal-questions-submit-error-stable-anyexternalcallerrortype-anyexternalcallerrortype-11122>`

    Transform an external\-call error type into an AnyExternalCallErrorType

  .. _function-daml-script-internal-questions-submit-error-fromanyexternalcallerrortype-35514:

  `fromAnyExternalCallErrorType <function-daml-script-internal-questions-submit-error-fromanyexternalcallerrortype-35514_>`_
    \: :ref:`AnyExternalCallErrorType <type-daml-script-internal-questions-submit-error-stable-anyexternalcallerrortype-anyexternalcallerrortype-11122>` \-\> `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ e

    Attempt to transform an AnyExternalCallErrorType into a specific external\-call error type, gives ``None`` if the underlying type of the AnyExternalCallErrorType
    does not match\.

  **instance** `IsExternalCallErrorType <class-daml-script-internal-questions-submit-error-isexternalcallerrortype-49854_>`_ :ref:`ExecutionFailedExternalCallError <type-daml-script-internal-questions-submit-error-executionfailedexternalcallerror-27747>`

  **instance** `IsExternalCallErrorType <class-daml-script-internal-questions-submit-error-isexternalcallerrortype-49854_>`_ :ref:`InvalidOutputExternalCallError <type-daml-script-internal-questions-submit-error-invalidoutputexternalcallerror-81292>`

  **instance** `IsExternalCallErrorType <class-daml-script-internal-questions-submit-error-isexternalcallerrortype-49854_>`_ :ref:`PreparationFailedExternalCallError <type-daml-script-internal-questions-submit-error-preparationfailedexternalcallerror-68134>`

.. _class-daml-script-internal-questions-submit-error-isdeverrortype-77141:

**class** `IsDevErrorType <class-daml-script-internal-questions-submit-error-isdeverrortype-77141_>`_ e **where**

  Errors that will be promoted to SubmitError once stable \- code needs to be kept in sync with SubmitError\.scala
  Allows casting AnyDevErrorType into and out of specific dev error types\.

  .. _function-daml-script-internal-questions-submit-error-toanydeverrortype-71882:

  `toAnyDevErrorType <function-daml-script-internal-questions-submit-error-toanydeverrortype-71882_>`_
    \: e \-\> :ref:`AnyDevErrorType <type-daml-script-internal-questions-submit-error-stable-anydeverrortype-anydeverrortype-93864>`

    Transform a dev error type into an AnyDevErrorType

  .. _function-daml-script-internal-questions-submit-error-fromanydeverrortype-55897:

  `fromAnyDevErrorType <function-daml-script-internal-questions-submit-error-fromanydeverrortype-55897_>`_
    \: :ref:`AnyDevErrorType <type-daml-script-internal-questions-submit-error-stable-anydeverrortype-anydeverrortype-93864>` \-\> `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ e

    Attempt to transform an AnyDevErrorType into a specific dev error type, gives ``None`` if the underlying type of the AnyDevErrorType
    does not match\.

  **instance** `IsDevErrorType <class-daml-script-internal-questions-submit-error-isdeverrortype-77141_>`_ :ref:`ChoiceGuardFailedDevError <type-daml-script-internal-questions-submit-error-choiceguardfaileddeverror-32850>`

  **instance** `IsDevErrorType <class-daml-script-internal-questions-submit-error-isdeverrortype-77141_>`_ :ref:`UnknownNewFeatureDevError <type-daml-script-internal-questions-submit-error-unknownnewfeaturedeverror-77479>`

Data Types
----------

.. _type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182:

**data** `UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_

  The rights of a user\.

  .. _constr-daml-script-internal-questions-usermanagement-stable-userright-participantadmin-26407:

  `ParticipantAdmin <constr-daml-script-internal-questions-usermanagement-stable-userright-participantadmin-26407_>`_


  .. _constr-daml-script-internal-questions-usermanagement-stable-userright-canactas-18221:

  `CanActAs <constr-daml-script-internal-questions-usermanagement-stable-userright-canactas-18221_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_


  .. _constr-daml-script-internal-questions-usermanagement-stable-userright-canreadas-58476:

  `CanReadAs <constr-daml-script-internal-questions-usermanagement-stable-userright-canreadas-58476_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_


  .. _constr-daml-script-internal-questions-usermanagement-stable-userright-canreadasanyparty-48466:

  `CanReadAsAnyParty <constr-daml-script-internal-questions-usermanagement-stable-userright-canreadasanyparty-48466_>`_


  .. _constr-daml-script-internal-questions-usermanagement-stable-userright-canexecuteas-94652:

  `CanExecuteAs <constr-daml-script-internal-questions-usermanagement-stable-userright-canexecuteas-94652_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_


  .. _constr-daml-script-internal-questions-usermanagement-stable-userright-canexecuteasanyparty-47374:

  `CanExecuteAsAnyParty <constr-daml-script-internal-questions-usermanagement-stable-userright-canexecuteasanyparty-47374_>`_


  .. _constr-daml-script-internal-questions-usermanagement-stable-userright-canactasanyparty-38827:

  `CanActAsAnyParty <constr-daml-script-internal-questions-usermanagement-stable-userright-canactasanyparty-38827_>`_


.. _type-daml-script-internal-questions-usermanagement-stable-usernotfound-usernotfound-64170:

**data** `UserNotFound <type-daml-script-internal-questions-usermanagement-stable-usernotfound-usernotfound-64170_>`_

  Thrown if a user cannot be located for a given user identifier\.

  .. _constr-daml-script-internal-questions-usermanagement-stable-usernotfound-usernotfound-73195:

  `UserNotFound <constr-daml-script-internal-questions-usermanagement-stable-usernotfound-usernotfound-73195_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_
         -

.. _type-daml-script-internal-questions-usermanagement-stable-user-user-13636:

**data** `User <type-daml-script-internal-questions-usermanagement-stable-user-user-13636_>`_

  User\-info record for a user in the user management service\.

  .. _constr-daml-script-internal-questions-usermanagement-stable-user-user-97149:

  `User <constr-daml-script-internal-questions-usermanagement-stable-user-user-97149_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_
         -
       * - primaryParty
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         -

.. _type-daml-script-internal-questions-usermanagement-stable-useralreadyexists-useralreadyexists-44214:

**data** `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-stable-useralreadyexists-useralreadyexists-44214_>`_

  Thrown if a user to be created already exists\.

  .. _constr-daml-script-internal-questions-usermanagement-stable-useralreadyexists-useralreadyexists-20731:

  `UserAlreadyExists <constr-daml-script-internal-questions-usermanagement-stable-useralreadyexists-useralreadyexists-20731_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_
         -

.. _type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234:

**data** `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_

  Identifier for a user in the user management service\.

  .. _constr-daml-script-internal-questions-usermanagement-stable-userid-userid-78675:

  `UserId <constr-daml-script-internal-questions-usermanagement-stable-userid-userid-78675_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_


.. _type-daml-script-internal-questions-usermanagement-stable-invaliduserid-invaliduserid-33910:

**data** `InvalidUserId <type-daml-script-internal-questions-usermanagement-stable-invaliduserid-invaliduserid-33910_>`_

  Thrown if text for a user identifier does not conform to the format restriction\.

  .. _constr-daml-script-internal-questions-usermanagement-stable-invaliduserid-invaliduserid-28115:

  `InvalidUserId <constr-daml-script-internal-questions-usermanagement-stable-invaliduserid-invaliduserid-28115_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - m
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-submit-concurrentsubmits-82688:

**data** `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a

  Applicative that allows for multiple concurrent transaction submissions
  See ``concurrently`` for usage of this type\.

  .. _constr-daml-script-internal-questions-submit-concurrentsubmits-49827:

  `ConcurrentSubmits <constr-daml-script-internal-questions-submit-concurrentsubmits-49827_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - submits
         - \[Submission\]
         -
       * - continue
         - \[`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824>` (\[CommandResult\], `TransactionTree <type-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-42393_>`_)\] \-\> a
         -

.. _type-daml-script-internal-questions-submit-stable-packageid-packageid-23442:

**data** `PackageId <type-daml-script-internal-questions-submit-stable-packageid-packageid-23442_>`_

  Package\-id newtype for package preference

  .. _constr-daml-script-internal-questions-submit-stable-packageid-packageid-37419:

  `PackageId <constr-daml-script-internal-questions-submit-stable-packageid-packageid-37419_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_


.. _type-daml-script-internal-questions-submit-stable-submitoptions-submitoptions-27150:

**data** `SubmitOptions <type-daml-script-internal-questions-submit-stable-submitoptions-submitoptions-27150_>`_

  Options to detemine the stakeholders of a transaction, as well as disclosures\.
  Intended to be specified using the ``actAs``, ``readAs`` and ``disclose`` builders, combined using the Semigroup concat ``(<>)`` operator\.

  .. code-block:: daml

    actAs alice <> readAs [alice, bob] <> disclose myContract


  Note that actAs and readAs follows the same party derivation rules as ``signatory``, see their docs for examples\.
  All submissions must specify at least one ``actAs`` party, else a runtime error will be thrown\.
  A minimum submission may look like

  .. code-block:: daml

    actAs alice `submit` createCmd MyContract with party = alice


  For backwards compatibility, a single or set of parties can be provided in place of the ``SubmitOptions`` to
  ``submit``, which will represent the ``actAs`` field\.
  The above example could be reduced to

  .. code-block:: daml

    alice `submit` createCmd MyContract with party = alice

.. _type-daml-script-internal-questions-transactiontree-stable-treeindex-exercisedindexpayload-41153:

**data** `ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-stable-treeindex-exercisedindexpayload-41153_>`_ t

  .. _constr-daml-script-internal-questions-transactiontree-stable-treeindex-exercisedindexpayload-19398:

  `ExercisedIndexPayload <constr-daml-script-internal-questions-transactiontree-stable-treeindex-exercisedindexpayload-19398_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - templateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - choice
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - offset
         - `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -
       * - child
         - `TreeIndex <type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841_>`_ t
         -

.. _type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841:

**data** `TreeIndex <type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841_>`_ t

  .. _constr-daml-script-internal-questions-transactiontree-stable-treeindex-createdindex-47483:

  `CreatedIndex <constr-daml-script-internal-questions-transactiontree-stable-treeindex-createdindex-47483_>`_ (`CreatedIndexPayload <type-daml-script-internal-questions-transactiontree-stable-createdindexpayload-createdindexpayload-2057_>`_ t)


  .. _constr-daml-script-internal-questions-transactiontree-stable-treeindex-exercisedindex-81579:

  `ExercisedIndex <constr-daml-script-internal-questions-transactiontree-stable-treeindex-exercisedindex-81579_>`_ (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-stable-treeindex-exercisedindexpayload-41153_>`_ t)


.. _type-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-42393:

**data** `TransactionTree <type-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-42393_>`_

  .. _constr-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-30690:

  `TransactionTree <constr-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-30690_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - rootEvents
         - \[`TreeEvent <type-daml-script-internal-questions-transactiontree-stable-treeevent-treeevent-40673_>`_\]
         -

.. _type-daml-script-internal-questions-transactiontree-stable-treeevent-exercised-3007:

**data** `Exercised <type-daml-script-internal-questions-transactiontree-stable-treeevent-exercised-3007_>`_

  .. _constr-daml-script-internal-questions-transactiontree-stable-treeevent-exercised-91860:

  `Exercised <constr-daml-script-internal-questions-transactiontree-stable-treeevent-exercised-91860_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_
         -
       * - choice
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - argument
         - `AnyChoice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_
         -
       * - childEvents
         - \[`TreeEvent <type-daml-script-internal-questions-transactiontree-stable-treeevent-treeevent-40673_>`_\]
         -

.. _type-daml-script-internal-questions-transactiontree-stable-treeevent-treeevent-40673:

**data** `TreeEvent <type-daml-script-internal-questions-transactiontree-stable-treeevent-treeevent-40673_>`_

  .. _constr-daml-script-internal-questions-transactiontree-stable-treeevent-createdevent-12647:

  `CreatedEvent <constr-daml-script-internal-questions-transactiontree-stable-treeevent-createdevent-12647_>`_ `Created <type-daml-script-internal-questions-transactiontree-stable-created-created-78249_>`_


  .. _constr-daml-script-internal-questions-transactiontree-stable-treeevent-exercisedevent-95955:

  `ExercisedEvent <constr-daml-script-internal-questions-transactiontree-stable-treeevent-exercisedevent-95955_>`_ `Exercised <type-daml-script-internal-questions-transactiontree-stable-treeevent-exercised-3007_>`_


.. _type-daml-script-internal-questions-transactiontree-stable-createdindexpayload-createdindexpayload-2057:

**data** `CreatedIndexPayload <type-daml-script-internal-questions-transactiontree-stable-createdindexpayload-createdindexpayload-2057_>`_ t

  .. _constr-daml-script-internal-questions-transactiontree-stable-createdindexpayload-createdindexpayload-16810:

  `CreatedIndexPayload <constr-daml-script-internal-questions-transactiontree-stable-createdindexpayload-createdindexpayload-16810_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - templateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - offset
         - `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -

.. _type-daml-script-internal-questions-transactiontree-stable-created-created-78249:

**data** `Created <type-daml-script-internal-questions-transactiontree-stable-created-created-78249_>`_

  .. _constr-daml-script-internal-questions-transactiontree-stable-created-created-59458:

  `Created <constr-daml-script-internal-questions-transactiontree-stable-created-created-59458_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_
         -
       * - argument
         - `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -

.. _type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288:

**data** `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  .. _constr-daml-script-internal-questions-util-stable-anycontractid-anycontractid-71797:

  `AnyContractId <constr-daml-script-internal-questions-util-stable-anycontractid-anycontractid-71797_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - templateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - contractId
         - `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ()
         -

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"additionalInfoCid\" :ref:`ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"coid\" :ref:`AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"coid\" :ref:`ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" :ref:`ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" :ref:`ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" :ref:`ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" :ref:`DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractId\" :ref:`WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"localVerdictLockedContracts\" :ref:`LocalVerdictLockedContractsSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-33196>` \[`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_\]

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"mCoid\" :ref:`TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_)

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"additionalInfoCid\" :ref:`ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"coid\" :ref:`AuthenticationFailedUpgradeError <type-daml-script-internal-questions-submit-error-authenticationfailedupgradeerror-46768>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"coid\" :ref:`ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" :ref:`ContractDoesNotImplementInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementinterfacesubmiterror-10383>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" :ref:`ContractDoesNotImplementRequiringInterfaceSubmitError <type-daml-script-internal-questions-submit-error-contractdoesnotimplementrequiringinterfacesubmiterror-56064>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" :ref:`ContractHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-contracthashingerrorsubmiterror-19860>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" :ref:`DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractId\" :ref:`WronglyTypedContractSubmitError <type-daml-script-internal-questions-submit-error-wronglytypedcontractsubmiterror-35056>` `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"localVerdictLockedContracts\" :ref:`LocalVerdictLockedContractsSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedcontractssubmiterror-33196>` \[`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"mCoid\" :ref:`TranslationFailedUpgradeError <type-daml-script-internal-questions-submit-error-translationfailedupgradeerror-57244>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_)

.. _type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562:

**data** `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_

  Participant name for multi\-participant script runs to address a specific participant

  .. _constr-daml-script-internal-questions-partymanagement-stable-participantname-participantname-84125:

  `ParticipantName <constr-daml-script-internal-questions-partymanagement-stable-participantname-participantname-84125_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - participantName
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-partymanagement-stable-partyidhint-partyidhint-41530:

**data** `PartyIdHint <type-daml-script-internal-questions-partymanagement-stable-partyidhint-partyidhint-41530_>`_

  A hint to the backing participant what party id to allocate\.
  Must be a valid PartyIdString (as described in @value\.proto@)\.

  .. _constr-daml-script-internal-questions-partymanagement-stable-partyidhint-partyidhint-54741:

  `PartyIdHint <constr-daml-script-internal-questions-partymanagement-stable-partyidhint-partyidhint-54741_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - partyIdHint
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-partymanagement-stable-partydetails-partydetails-97882:

**data** `PartyDetails <type-daml-script-internal-questions-partymanagement-stable-partydetails-partydetails-97882_>`_

  The party details returned by the party management service\.

  .. _constr-daml-script-internal-questions-partymanagement-stable-partydetails-partydetails-2209:

  `PartyDetails <constr-daml-script-internal-questions-partymanagement-stable-partydetails-partydetails-2209_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - party
         - `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         - Party id
       * - isLocal
         - `Bool <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_
         - True if party is hosted by the backing participant\.

.. _type-daml-script-internal-questions-crypto-text-privatekeyhex-82732:

**type** `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_
  \= `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  A DER formatted private key to be used for ECDSA message signing

.. _type-daml-script-internal-questions-crypto-text-stable-secp256k1keypair-secp256k1keypair-89485:

**data** `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-stable-secp256k1keypair-secp256k1keypair-89485_>`_

  Secp256k1 key pair generated by ``secp256k1generatekeypair`` for testing\.

  .. _constr-daml-script-internal-questions-crypto-text-stable-secp256k1keypair-secp256k1keypair-76534:

  `Secp256k1KeyPair <constr-daml-script-internal-questions-crypto-text-stable-secp256k1keypair-secp256k1keypair-76534_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - privateKey
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - publicKey
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-commands-stable-disclosure-disclosure-17640:

**data** `Disclosure <type-daml-script-internal-questions-commands-stable-disclosure-disclosure-17640_>`_

  Contract disclosures which can be acquired via ``queryDisclosure``

  .. _constr-daml-script-internal-questions-commands-stable-disclosure-disclosure-6181:

  `Disclosure <constr-daml-script-internal-questions-commands-stable-disclosure-disclosure-6181_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - templateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - contractId
         - `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ()
         -
       * - blob
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-commands-stable-commands-commands-95086:

**data** `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a

  This is used to build up the commands sent as part of ``submit``\.
  If you enable the ``ApplicativeDo`` extension by adding
  ``{-# LANGUAGE ApplicativeDo #-}`` at the top of your file, you can
  use ``do``\-notation but the individual commands must not depend
  on each other and the last statement in a ``do`` block
  must be of the form ``return expr`` or ``pure expr``\.

  .. _constr-daml-script-internal-questions-commands-stable-commands-commands-45295:

  `Commands <constr-daml-script-internal-questions-commands-stable-commands-commands-45295_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - commands
         - \[CommandWithMeta\]
         -
       * - continue
         - \[CommandResult\] \-\> a
         -

.. _type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404:

**data** `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  Existential contract key type that can wrap an arbitrary contract key\.

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractKey\" :ref:`ContractKeyNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-26927>` `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"contractKey\" :ref:`InconsistentContractKeySubmitError <type-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-13545>` `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"duplicateContractKey\" :ref:`DuplicateContractKeySubmitError <type-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-30134>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_)

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"expectedKey\" :ref:`DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935>` `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"failedTemplateKey\" :ref:`FetchEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-95321>` `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"localVerdictLockedKeys\" :ref:`LocalVerdictLockedKeysSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-25684>` \[`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_\]

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"originalKeyOpt\" :ref:`ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"recomputedKeyOpt\" :ref:`ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractKey\" :ref:`ContractKeyNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractkeynotfoundsubmiterror-26927>` `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"contractKey\" :ref:`InconsistentContractKeySubmitError <type-daml-script-internal-questions-submit-error-inconsistentcontractkeysubmiterror-13545>` `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"duplicateContractKey\" :ref:`DuplicateContractKeySubmitError <type-daml-script-internal-questions-submit-error-duplicatecontractkeysubmiterror-30134>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_)

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"expectedKey\" :ref:`DisclosedContractKeyHashingErrorSubmitError <type-daml-script-internal-questions-submit-error-disclosedcontractkeyhashingerrorsubmiterror-24935>` `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"failedTemplateKey\" :ref:`FetchEmptyContractKeyMaintainersSubmitError <type-daml-script-internal-questions-submit-error-fetchemptycontractkeymaintainerssubmiterror-95321>` `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"localVerdictLockedKeys\" :ref:`LocalVerdictLockedKeysSubmitError <type-daml-script-internal-questions-submit-error-localverdictlockedkeyssubmiterror-25684>` \[`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"originalKeyOpt\" :ref:`ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"recomputedKeyOpt\" :ref:`ValidationFailedUpgradeError <type-daml-script-internal-questions-submit-error-validationfailedupgradeerror-84297>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]))

.. _type-daml-script-internal-lowlevel-stable-script-script-12809:

**data** `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ a

  This is the type of A Daml script\. ``Script`` is an instance of ``Action``,
  so you can use ``do`` notation\.

  .. _constr-daml-script-internal-lowlevel-stable-script-script-14144:

  `Script <constr-daml-script-internal-lowlevel-stable-script-script-14144_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - runScript
         - () \-\> Free ScriptF (a, ())
         - HIDE We use an inlined StateT () to separate evaluation of something of type Script from execution and to ensure proper sequencing of evaluation\. This is mainly so that ``debug`` does something slightly more sensible\.
       * - dummy
         - ()
         - HIDE Dummy field to make sure damlc does not consider this an old\-style typeclass\.

Functions
---------

.. _function-daml-script-internal-questions-usermanagement-useridtotext-75939:

`userIdToText <function-daml-script-internal-questions-usermanagement-useridtotext-75939_>`_
  \: `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  Extract the name\-text from a user identitifer\.

.. _function-daml-script-internal-questions-usermanagement-validateuserid-51917:

`validateUserId <function-daml-script-internal-questions-usermanagement-validateuserid-51917_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_

  Construct a user identifer from text\. May throw InvalidUserId\.

.. _function-daml-script-internal-questions-usermanagement-createuser-37948:

`createUser <function-daml-script-internal-questions-usermanagement-createuser-37948_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `User <type-daml-script-internal-questions-usermanagement-stable-user-user-13636_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\] \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ ()

  Create a user with the given rights\. May throw UserAlreadyExists\.

.. _function-daml-script-internal-questions-usermanagement-createuseron-3905:

`createUserOn <function-daml-script-internal-questions-usermanagement-createuseron-3905_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `User <type-daml-script-internal-questions-usermanagement-stable-user-user-13636_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\] \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ ()

  Create a user with the given rights on the given participant\. May throw UserAlreadyExists\.

.. _function-daml-script-internal-questions-usermanagement-getuser-5077:

`getUser <function-daml-script-internal-questions-usermanagement-getuser-5077_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `User <type-daml-script-internal-questions-usermanagement-stable-user-user-13636_>`_

  Fetch a user record by user id\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-getuseron-1968:

`getUserOn <function-daml-script-internal-questions-usermanagement-getuseron-1968_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `User <type-daml-script-internal-questions-usermanagement-stable-user-user-13636_>`_

  Fetch a user record by user id from the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-listallusers-63416:

`listAllUsers <function-daml-script-internal-questions-usermanagement-listallusers-63416_>`_
  \: `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`User <type-daml-script-internal-questions-usermanagement-stable-user-user-13636_>`_\]

  List all users\. This function may make multiple calls to underlying paginated ledger API\.

.. _function-daml-script-internal-questions-usermanagement-listalluserson-20857:

`listAllUsersOn <function-daml-script-internal-questions-usermanagement-listalluserson-20857_>`_
  \: `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`User <type-daml-script-internal-questions-usermanagement-stable-user-user-13636_>`_\]

  List all users on the given participant\. This function may make multiple calls to underlying paginated ledger API\.

.. _function-daml-script-internal-questions-usermanagement-grantuserrights-87478:

`grantUserRights <function-daml-script-internal-questions-usermanagement-grantuserrights-87478_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\] \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\]

  Grant rights to a user\. Returns the rights that have been newly granted\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-grantuserrightson-91259:

`grantUserRightsOn <function-daml-script-internal-questions-usermanagement-grantuserrightson-91259_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\] \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\]

  Grant rights to a user on the given participant\. Returns the rights that have been newly granted\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-revokeuserrights-85325:

`revokeUserRights <function-daml-script-internal-questions-usermanagement-revokeuserrights-85325_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\] \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\]

  Revoke rights for a user\. Returns the revoked rights\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-revokeuserrightson-21608:

`revokeUserRightsOn <function-daml-script-internal-questions-usermanagement-revokeuserrightson-21608_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\] \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\]

  Revoke rights for a user on the given participant\. Returns the revoked rights\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-deleteuser-2585:

`deleteUser <function-daml-script-internal-questions-usermanagement-deleteuser-2585_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ ()

  Delete a user\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-deleteuseron-74248:

`deleteUserOn <function-daml-script-internal-questions-usermanagement-deleteuseron-74248_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ ()

  Delete a user on the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-listuserrights-50525:

`listUserRights <function-daml-script-internal-questions-usermanagement-listuserrights-50525_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\]

  List the rights of a user\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-listuserrightson-11796:

`listUserRightsOn <function-daml-script-internal-questions-usermanagement-listuserrightson-11796_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-stable-userright-userright-81182_>`_\]

  List the rights of a user on the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-submituser-29476:

`submitUser <function-daml-script-internal-questions-usermanagement-submituser-29476_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ a

  Submit the commands with the actAs and readAs claims granted to a user\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-submituseron-39337:

`submitUserOn <function-daml-script-internal-questions-usermanagement-submituseron-39337_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-stable-userid-userid-57234_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ a

  Submit the commands with the actAs and readAs claims granted to the user on the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-time-settime-32330:

`setTime <function-daml-script-internal-questions-time-settime-32330_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Time <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ ()

  Set the time via the time service\.

  This is only supported in Daml Studio and ``dpm test`` as well as
  when running over the gRPC API against a ledger in static time mode\.

  Note that the ledger time service does not support going backwards in time\.
  However, you can go back in time in Daml Studio\.

.. _function-daml-script-internal-questions-time-sleep-58882:

`sleep <function-daml-script-internal-questions-time-sleep-58882_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `RelTime <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Time.html#type-da-time-types-reltime-23082>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ ()

  Sleep for the given duration\.

  This is primarily useful in tests
  where you repeatedly call ``query`` until a certain state is reached\.

  Note that this will sleep for the same duration in both wall clock and static time mode\.

.. _function-daml-script-internal-questions-time-passtime-50024:

`passTime <function-daml-script-internal-questions-time-passtime-50024_>`_
  \: `RelTime <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Time.html#type-da-time-types-reltime-23082>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ ()

  Advance ledger time by the given interval\.

  This is only supported in Daml Studio and ``dpm test`` as well as
  when running over the gRPC API against a ledger in static time mode\.
  Note that this is not an atomic operation over the
  gRPC API so no other clients should try to change time while this is
  running\.

  Note that the ledger time service does not support going backwards in time\.
  However, you can go back in time in Daml Studio\.

.. _function-daml-script-internal-questions-submit-actas-76494:

`actAs <function-daml-script-internal-questions-submit-actas-76494_>`_
  \: `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ parties \=\> parties \-\> `SubmitOptions <type-daml-script-internal-questions-submit-stable-submitoptions-submitoptions-27150_>`_

  Builds a SubmitOptions with given actAs parties\.
  Any given submission must include at least one actAs party\.
  Note that the parties type is constrainted by ``IsParties``, allowing for specifying parties as any of the following\:

  .. code-block:: daml

    Party
    [Party]
    NonEmpty Party
    Set Party
    Optional Party

.. _function-daml-script-internal-questions-submit-readas-67481:

`readAs <function-daml-script-internal-questions-submit-readas-67481_>`_
  \: `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ parties \=\> parties \-\> `SubmitOptions <type-daml-script-internal-questions-submit-stable-submitoptions-submitoptions-27150_>`_

  Builds a SubmitOptions with given readAs parties\.
  A given submission may omit any readAs parties and still be valid\.
  Note that the parties type is constrainted by ``IsParties``, allowing for specifying parties as any of the following\:

  .. code-block:: daml

    Party
    [Party]
    NonEmpty Party
    Set Party
    Optional Party

.. _function-daml-script-internal-questions-submit-disclosemany-53386:

`discloseMany <function-daml-script-internal-questions-submit-disclosemany-53386_>`_
  \: \[`Disclosure <type-daml-script-internal-questions-commands-stable-disclosure-disclosure-17640_>`_\] \-\> `SubmitOptions <type-daml-script-internal-questions-submit-stable-submitoptions-submitoptions-27150_>`_

  Provides many Explicit Disclosures to the transaction\.

.. _function-daml-script-internal-questions-submit-disclose-59895:

`disclose <function-daml-script-internal-questions-submit-disclose-59895_>`_
  \: `Disclosure <type-daml-script-internal-questions-commands-stable-disclosure-disclosure-17640_>`_ \-\> `SubmitOptions <type-daml-script-internal-questions-submit-stable-submitoptions-submitoptions-27150_>`_

  Provides an Explicit Disclosure to the transaction\.

.. _function-daml-script-internal-questions-submit-packagepreference-25445:

`packagePreference <function-daml-script-internal-questions-submit-packagepreference-25445_>`_
  \: \[`PackageId <type-daml-script-internal-questions-submit-stable-packageid-packageid-23442_>`_\] \-\> `SubmitOptions <type-daml-script-internal-questions-submit-stable-submitoptions-submitoptions-27150_>`_

  Provide a package id selection preference for upgrades for a submission

.. _function-daml-script-internal-questions-submit-prefetchkeys-84998:

`prefetchKeys <function-daml-script-internal-questions-submit-prefetchkeys-84998_>`_
  \: \[`AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_\] \-\> `SubmitOptions <type-daml-script-internal-questions-submit-stable-submitoptions-submitoptions-27150_>`_

  Provide a list of contract keys to prefetch for a submission

.. _function-daml-script-internal-questions-submit-concurrently-75077:

`concurrently <function-daml-script-internal-questions-submit-concurrently-75077_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ a

  Allows for concurrent submission of transactions, using an applicative, similar to Commands\.
  Concurrently takes a computation in ``ConcurrentSubmits``, which supports all the existing ``submit`` functions
  that ``Script`` supports\. It however does not implement ``Action``, and thus does not support true binding and computation interdependence
  NOTE\: The submission order of transactions within ``concurrently`` is deterministic, this function is not intended to test contention\.
  It is only intended to allow faster submission of many unrelated transactions, by not waiting for completion for each transaction before
  sending the next\.
  Example\:

  .. code-block:: daml

    exerciseResult <- concurrently $ do
      alice `submit` createCmd ...
      res <- alice `submit` exerciseCmd ...
      bob `submit` createCmd ...
      pure res

.. _function-daml-script-internal-questions-submit-submitresultandtree-13546:

`submitResultAndTree <function-daml-script-internal-questions-submit-submitresultandtree-13546_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script (a, `TransactionTree <type-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-42393_>`_)

  Equivalent to ``submit`` but returns the result and the full transaction tree\.

.. _function-daml-script-internal-questions-submit-trysubmitresultandtree-33682:

`trySubmitResultAndTree <function-daml-script-internal-questions-submit-trysubmitresultandtree-33682_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824>` (a, `TransactionTree <type-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-42393_>`_))

  Equivalent to ``trySubmit`` but returns the result and the full transaction tree\.

.. _function-daml-script-internal-questions-submit-submitwitherror-52958:

`submitWithError <function-daml-script-internal-questions-submit-submitwitherror-52958_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script :ref:`SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824>`

  Equivalent to ``submitMustFail`` but returns the error thrown\.

.. _function-daml-script-internal-questions-submit-submit-5889:

`submit <function-daml-script-internal-questions-submit-submit-5889_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script a

  ``submit p cmds`` submits the commands ``cmds`` as a single transaction
  from party ``p`` and returns the value returned by ``cmds``\.
  The ``options`` field can either be any \"Parties\" like type (See ``IsParties``) or ``SubmitOptions``
  which allows for finer control over parameters of the submission\.

  If the transaction fails, ``submit`` also fails\.

.. _function-daml-script-internal-questions-submit-submitwithoptions-56152:

`submitWithOptions <function-daml-script-internal-questions-submit-submitwithoptions-56152_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script a

  .. warning::
    **DEPRECATED**\:

    | Daml 2\.9 compatibility helper, use 'submit' instead

.. _function-daml-script-internal-questions-submit-submittree-5925:

`submitTree <function-daml-script-internal-questions-submit-submittree-5925_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script `TransactionTree <type-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-42393_>`_

  Equivalent to ``submit`` but returns the full transaction tree\.

.. _function-daml-script-internal-questions-submit-trysubmit-23693:

`trySubmit <function-daml-script-internal-questions-submit-trysubmit-23693_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824>` a)

  Submit a transaction and recieve back either the result, or a ``SubmitError``\.
  In the majority of failures, this will not crash at runtime\.

.. _function-daml-script-internal-questions-submit-trysubmittree-68085:

`trySubmitTree <function-daml-script-internal-questions-submit-trysubmittree-68085_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824>` `TransactionTree <type-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-42393_>`_)

  Equivalent to ``trySubmit`` but returns the full transaction tree\.

.. _function-daml-script-internal-questions-submit-submitmustfail-63662:

`submitMustFail <function-daml-script-internal-questions-submit-submitmustfail-63662_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script ()

  ``submitMustFail p cmds`` submits the commands ``cmds`` as a single transaction
  from party ``p``\.
  See submitWithOptions for details on the ``options`` field

  It only succeeds if the submitting the transaction fails\.

.. _function-daml-script-internal-questions-submit-submitmustfailwithoptions-20017:

`submitMustFailWithOptions <function-daml-script-internal-questions-submit-submitmustfailwithoptions-20017_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script ()

  .. warning::
    **DEPRECATED**\:

    | Daml 2\.9 compatibility helper, use 'submitMustFail' instead

.. _function-daml-script-internal-questions-submit-submitmulti-45107:

`submitMulti <function-daml-script-internal-questions-submit-submitmulti-45107_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script a

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``submit``, ``actAs`` and ``readAs`` separately

  ``submitMulti actAs readAs cmds`` submits ``cmds`` as a single transaction
  authorized by ``actAs``\. Fetched contracts must be visible to at least
  one party in the union of actAs and readAs\.

  Note\: This behaviour can be achieved using ``submit (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-submitmultimustfail-77808:

`submitMultiMustFail <function-daml-script-internal-questions-submit-submitmultimustfail-77808_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script ()

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``submitMustFail``, ``actAs`` and ``readAs`` separately

  ``submitMultiMustFail actAs readAs cmds`` behaves like ``submitMulti actAs readAs cmds``
  but fails when ``submitMulti`` succeeds and the other way around\.

  Note\: This behaviour can be achieved using ``submitMustFail (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-submittreemulti-4879:

`submitTreeMulti <function-daml-script-internal-questions-submit-submittreemulti-4879_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script `TransactionTree <type-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-42393_>`_

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``submitTree``, ``actAs`` and ``readAs`` separately

  Equivalent to ``submitMulti`` but returns the full transaction tree\.

  Note\: This behaviour can be achieved using ``submitTree (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-trysubmitmulti-31939:

`trySubmitMulti <function-daml-script-internal-questions-submit-trysubmitmulti-31939_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> script (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824>` a)

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``trySubmit``, ``actAs`` and ``readAs`` separately

  Alternate version of ``trySubmit`` that allows specifying the actAs and readAs parties\.

  Note\: This behaviour can be achieved using ``trySubmit (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-trysubmitconcurrently-11443:

`trySubmitConcurrently <function-daml-script-internal-questions-submit-trysubmitconcurrently-11443_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> \[`Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a\] \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ :ref:`SubmitError <type-daml-script-internal-questions-submit-error-compatibility-submiterror-33824>` a\]

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``concurrent`` and ``trySubmit`` separately

.. _function-daml-script-internal-questions-submit-submitwithdisclosures-50120:

`submitWithDisclosures <function-daml-script-internal-questions-submit-submitwithdisclosures-50120_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> \[`Disclosure <type-daml-script-internal-questions-commands-stable-disclosure-disclosure-17640_>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ a

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``trySubmit`` and ``disclosures`` separately

.. _function-daml-script-internal-questions-submit-submitwithdisclosuresmustfail-28475:

`submitWithDisclosuresMustFail <function-daml-script-internal-questions-submit-submitwithdisclosuresmustfail-28475_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> \[`Disclosure <type-daml-script-internal-questions-commands-stable-disclosure-disclosure-17640_>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ ()

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``submitMustFail`` and ``disclosures`` separately

.. _function-daml-script-internal-questions-transactiontree-fromtree-1340:

`fromTree <function-daml-script-internal-questions-transactiontree-fromtree-1340_>`_
  \: `Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t \=\> `TransactionTree <type-daml-script-internal-questions-transactiontree-stable-transactiontree-transactiontree-42393_>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841_>`_ t \-\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t

  Finds the contract id of an event within a tree given a tree index
  Tree indices are created using the ``created(N)`` and ``exercised(N)`` builders
  which allow building \"paths\" within a transaction to a create node
  For example, ``exercisedN @MyTemplate1 "MyChoice" 2 $ createdN @MyTemplate2 1``
  would find the ``ContractId MyTemplate2`` of the second (0 index) create event under
  the 3rd exercise event of ``MyChoice`` from ``MyTemplate1``

.. _function-daml-script-internal-questions-transactiontree-created-56097:

`created <function-daml-script-internal-questions-transactiontree-created-56097_>`_
  \: `HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `TreeIndex <type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841_>`_ t

  Index for the first create event of a given template
  e\.g\. ``created @MyTemplate``

.. _function-daml-script-internal-questions-transactiontree-createdn-71930:

`createdN <function-daml-script-internal-questions-transactiontree-createdn-71930_>`_
  \: `HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841_>`_ t

  Index for the Nth create event of a given template
  e\.g\. ``createdN 2 @MyTemplate``
  ``created = createdN 0``

.. _function-daml-script-internal-questions-transactiontree-exercised-13349:

`exercised <function-daml-script-internal-questions-transactiontree-exercised-13349_>`_
  \: `HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841_>`_ t' \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841_>`_ t'

  Index for the first exercise of a given choice on a given template
  e\.g\. ``exercised @MyTemplate "MyChoice"``

.. _function-daml-script-internal-questions-transactiontree-exercisedn-70910:

`exercisedN <function-daml-script-internal-questions-transactiontree-exercisedn-70910_>`_
  \: `HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841_>`_ t' \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-stable-treeindex-treeindex-63841_>`_ t'

  Index for the Nth exercise of a given choice on a given template
  e\.g\. ``exercisedN @MyTemplate "MyChoice" 2``
  ``exercised c = exercisedN c 0``

.. _function-daml-script-internal-questions-util-fromanycontractid-11435:

`fromAnyContractId <function-daml-script-internal-questions-util-fromanycontractid-11435_>`_
  \: `Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t \=\> `AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288_>`_ \-\> `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t)

.. _function-daml-script-internal-questions-query-query-55941:

`query <function-daml-script-internal-questions-query-query-55941_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[(`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, t)\]

  Query the set of active contracts of the template
  that are visible to the given party\.

.. _function-daml-script-internal-questions-query-queryfilter-99157:

`queryFilter <function-daml-script-internal-questions-query-queryfilter-99157_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ c, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ c, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> (c \-\> `Bool <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_) \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[(`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ c, c)\]

  Query the set of active contracts of the template
  that are visible to the given party and match the given predicate\.

.. _function-daml-script-internal-questions-query-querycontractid-24166:

`queryContractId <function-daml-script-internal-questions-query-querycontractid-24166_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ t)

  Query for the contract with the given contract id\.

  Returns ``None`` if there is no active contract the party is a stakeholder on\.

  WARNING\: Over the gRPC backend this performs a linear search over all contracts of
  the same type, so only use this if the number of active contracts is small\.

  This is semantically equivalent to calling ``query``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-query-querydisclosure-12000:

`queryDisclosure <function-daml-script-internal-questions-query-querydisclosure-12000_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Disclosure <type-daml-script-internal-questions-commands-stable-disclosure-disclosure-17640_>`_)

  Queries a Disclosure for a given ContractId\. Same performance caveats apply as to ``queryContractId``\.

.. _function-daml-script-internal-questions-query-queryinterface-52085:

`queryInterface <function-daml-script-internal-questions-query-queryinterface-52085_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ i, `HasInterfaceView <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492>`_ i v, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[(`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ i, `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ v)\]

  Query the set of active contract views for an interface
  that are visible to the given party\.
  If the view function fails for a given contract id, The ``Optional v`` will be ``None``\.

.. _function-daml-script-internal-questions-query-queryinterfacecontractid-18438:

`queryInterfaceContractId <function-daml-script-internal-questions-query-queryinterfacecontractid-18438_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ i, `HasInterfaceView <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492>`_ i v, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ i \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ v)

  Query for the contract view with the given contract id\.

  Returns ``None`` if there is no active contract the party is a stakeholder on\.

  Returns ``None`` if the view function fails for the given contract id\.

  WARNING\: Over the gRPC backend this performs a linear search over all contracts of
  the same type, so only use this if the number of active contracts is small\.

  This is semantically equivalent to calling ``queryInterface``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-query-querybykey-184:

`queryByKey <function-daml-script-internal-questions-query-querybykey-184_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> k \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, t))

  Returns ``None`` if there is no active contract with the given key that
  the party is a stakeholder on\.

  WARNING\: Over the gRPC backend this performs a linear search over all contracts of
  the same type, so only use this if the number of active contracts is small\.

  This is semantically equivalent to calling ``query``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-query-querynbykey-54281:

`queryNByKey <function-daml-script-internal-questions-query-querynbykey-54281_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> k \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[(`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, t)\]

  Returns N contracts matching a key, returns empty list if the key is inactive
  Only available in LF 2\.3\+

  WARNING\: Over the gRPC backend this performs a linear search over all contracts of
  the same type, so only use this if the number of active contracts is small\.

  This is semantically equivalent to calling ``query``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-query-queryallbykey-81262:

`queryAllByKey <function-daml-script-internal-questions-query-queryallbykey-81262_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> k \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[(`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, t)\]

  Returns all contracts matching a key, returns empty list if the key is inactive
  Only available in LF 2\.3\+

  WARNING\: Over the gRPC backend this performs a linear search over all contracts of
  the same type, so only use this if the number of active contracts is small\.

  This is semantically equivalent to calling ``query``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-partymanagement-allocateparty-4749:

`allocateParty <function-daml-script-internal-questions-partymanagement-allocateparty-4749_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name
  using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-allocatepartywithhint-96426:

`allocatePartyWithHint <function-daml-script-internal-questions-partymanagement-allocatepartywithhint-96426_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `PartyIdHint <type-daml-script-internal-questions-partymanagement-stable-partyidhint-partyidhint-41530_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  .. warning::
    **DEPRECATED**\:

    | Daml 3\.3 compatibility helper, use 'allocatePartyByHint' instead of 'allocatePartyWithHint'

.. _function-daml-script-internal-questions-partymanagement-allocatepartybyhint-55067:

`allocatePartyByHint <function-daml-script-internal-questions-partymanagement-allocatepartybyhint-55067_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `PartyIdHint <type-daml-script-internal-questions-partymanagement-stable-partyidhint-partyidhint-41530_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given id hint
  using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-allocatepartyon-59020:

`allocatePartyOn <function-daml-script-internal-questions-partymanagement-allocatepartyon-59020_>`_
  \: `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name
  on the specified participant using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-allocatepartywithhinton-11859:

`allocatePartyWithHintOn <function-daml-script-internal-questions-partymanagement-allocatepartywithhinton-11859_>`_
  \: `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `PartyIdHint <type-daml-script-internal-questions-partymanagement-stable-partyidhint-partyidhint-41530_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  .. warning::
    **DEPRECATED**\:

    | Daml 3\.3 compatibility helper, use 'allocatePartyByHintOn' instead of 'allocatePartyWithHintOn'

.. _function-daml-script-internal-questions-partymanagement-allocatepartybyhinton-5218:

`allocatePartyByHintOn <function-daml-script-internal-questions-partymanagement-allocatepartybyhinton-5218_>`_
  \: `PartyIdHint <type-daml-script-internal-questions-partymanagement-stable-partyidhint-partyidhint-41530_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given id hint
  on the specified participant using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-listknownparties-55540:

`listKnownParties <function-daml-script-internal-questions-partymanagement-listknownparties-55540_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`PartyDetails <type-daml-script-internal-questions-partymanagement-stable-partydetails-partydetails-97882_>`_\]

  List the parties known to the default participant\.

.. _function-daml-script-internal-questions-partymanagement-listknownpartieson-55333:

`listKnownPartiesOn <function-daml-script-internal-questions-partymanagement-listknownpartieson-55333_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562_>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ \[`PartyDetails <type-daml-script-internal-questions-partymanagement-stable-partydetails-partydetails-97882_>`_\]

  List the parties known to the given participant\.

.. _function-daml-script-internal-questions-exceptions-trytoeither-58773:

`tryToEither <function-daml-script-internal-questions-exceptions-trytoeither-58773_>`_
  \: (() \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ t) \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `AnyException <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_ t)

  Named version of the ``try catch`` behaviour of Daml\-Script\.
  Note that this is no more powerful than ``try catch`` in daml\-script, and will not catch exceptions in submissions\.
  (Use ``trySubmit`` for this)
  Input computation is deferred to catch pure exceptions

.. _function-daml-script-internal-questions-exceptions-tryfailurestatus-576:

`tryFailureStatus <function-daml-script-internal-questions-exceptions-tryfailurestatus-576_>`_
  \: `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `FailureStatus <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Fail.html#type-da-internal-fail-types-failurestatus-69615>`_ a)

  Runs a script for a result\. If it fails either by Daml Exceptions or ``failWithStatus``, returns the
  ``FailureStatus`` that a Canton Ledger would return\.

.. _function-daml-script-internal-questions-crypto-text-secp256k1signwithecdsaonly-99207:

`secp256k1signWithEcdsaOnly <function-daml-script-internal-questions-crypto-text-secp256k1signwithecdsaonly-99207_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_ \-\> `BytesHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `BytesHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  Using a DER formatted private key (encoded as a hex string) use Secp256k1 to sign a hex encoded string message\.

  Note that this implementation uses a random source with a fixed PRNG and seed, ensuring it behaves deterministically during testing\.

  For example, CCTP attestation services may be mocked in daml\-script code\.

.. _function-daml-script-internal-questions-crypto-text-secp256k1sign-72886:

`secp256k1sign <function-daml-script-internal-questions-crypto-text-secp256k1sign-72886_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_ \-\> `BytesHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_ \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `BytesHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  Using a DER formatted private key (encoded as a hex string) use Secp256k1 to sign a SHA256 digest of a hex encoded string message\.

  Note that this implementation uses a random source with a fixed PRNG and seed, ensuring it behaves deterministically during testing\.

  For example, CCTP attestation services may be mocked in daml\-script code\.

.. _function-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-90200:

`secp256k1generatekeypair <function-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-90200_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-stable-secp256k1keypair-secp256k1keypair-89485_>`_

  Generate DER formatted Secp256k1 public/private key pairs\.

.. _function-daml-script-internal-questions-commands-toanycontractkey-91361:

`toAnyContractKey <function-daml-script-internal-questions-commands-toanycontractkey-91361_>`_
  \: (`HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t, `TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k) \=\> k \-\> `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_

  Wrap a contract key in ``AnyContractKey``\.

  You must pass the template type ``t`` using an explicit type application\.
  For example ``toAnyContractKey @Proposal k``\.

.. _function-daml-script-internal-questions-commands-fromanycontractkey-42688:

`fromAnyContractKey <function-daml-script-internal-questions-commands-fromanycontractkey-42688_>`_
  \: (`HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t, `TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k) \=\> `AnyContractKey <type-daml-script-internal-questions-commands-stable-anycontractkey-anycontractkey-21404_>`_ \-\> `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ k

  Extract the underlying key from ``AnyContractKey`` if the template and
  choice types match, or return ``None``\.

  You must pass the template type ``t`` using an explicit type application\.
  For example ``fromAnyContractKey @Proposal k``\.

.. _function-daml-script-internal-questions-commands-createcmd-46830:

`createCmd <function-daml-script-internal-questions-commands-createcmd-46830_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t)

  Create a contract of the given template\.

.. _function-daml-script-internal-questions-commands-exercisecmd-7438:

`exerciseCmd <function-daml-script-internal-questions-commands-exercisecmd-7438_>`_
  \: `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r \=\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ r

  Exercise a choice on the given contract\.

.. _function-daml-script-internal-questions-commands-exercisebykeycmd-80697:

`exerciseByKeyCmd <function-daml-script-internal-questions-commands-exercisebykeycmd-80697_>`_
  \: (`TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r) \=\> k \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ r

  Exercise a choice on the contract with the given key\.

.. _function-daml-script-internal-questions-commands-createandexercisewithcidcmd-21289:

`createAndExerciseWithCidCmd <function-daml-script-internal-questions-commands-createandexercisewithcidcmd-21289_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, r)

  Create a contract and exercise a choice on it in the same transaction, returns the created ContractId, and the choice result\.

.. _function-daml-script-internal-questions-commands-createandexercisecmd-8600:

`createAndExerciseCmd <function-daml-script-internal-questions-commands-createandexercisecmd-8600_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ r

  Create a contract and exercise a choice on it in the same transaction, returns only the choice result\.

.. _function-daml-script-internal-questions-commands-createexactcmd-86998:

`createExactCmd <function-daml-script-internal-questions-commands-createexactcmd-86998_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t)

  Create a contract of the given template, using the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-exerciseexactcmd-18398:

`exerciseExactCmd <function-daml-script-internal-questions-commands-exerciseexactcmd-18398_>`_
  \: `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r \=\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ r

  Exercise a choice on the given contract, using the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-exercisebykeyexactcmd-4555:

`exerciseByKeyExactCmd <function-daml-script-internal-questions-commands-exercisebykeyexactcmd-4555_>`_
  \: (`TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r) \=\> k \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ r

  Exercise a choice on the contract with the given key, using the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-createandexercisewithcidexactcmd-15363:

`createAndExerciseWithCidExactCmd <function-daml-script-internal-questions-commands-createandexercisewithcidexactcmd-15363_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, r)

  Create a contract and exercise a choice on it in the same transaction, returns the created ContractId, and the choice result\.
  Uses the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-createandexerciseexactcmd-54956:

`createAndExerciseExactCmd <function-daml-script-internal-questions-commands-createandexerciseexactcmd-54956_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ r

  Create a contract and exercise a choice on it in the same transaction, returns only the choice result\.

.. _function-daml-script-internal-questions-commands-archivecmd-47203:

`archiveCmd <function-daml-script-internal-questions-commands-archivecmd-47203_>`_
  \: `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t `Archive <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-archive-15178>`_ () \=\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> `Commands <type-daml-script-internal-questions-commands-stable-commands-commands-95086_>`_ ()

  Archive the given contract\.

  ``archiveCmd cid`` is equivalent to ``exerciseCmd cid Archive``\.

.. _function-daml-script-internal-lowlevel-script-65113:

`script <function-daml-script-internal-lowlevel-script-65113_>`_
  \: `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-stable-script-script-12809_>`_ a

  Convenience helper to declare you are writing a Script\.

  This is only useful for readability and to improve type inference\.
  Any expression of type ``Script a`` is a valid script regardless of whether
  it is implemented using ``script`` or not\.

