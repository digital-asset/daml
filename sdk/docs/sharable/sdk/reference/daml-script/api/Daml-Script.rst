.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
    \: options \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

.. _class-daml-script-internal-questions-submit-scriptsubmit-55101:

**class** `Applicative <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-prelude-applicative-9257>`_ script \=\> `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script **where**

  Defines an applicative that can run transaction submissions\. Usually this is simply ``Script``\.

  .. _function-daml-script-internal-questions-submit-liftsubmission-99954:

  `liftSubmission <function-daml-script-internal-questions-submit-liftsubmission-99954_>`_
    \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a \-\> script a

Data Types
----------

.. _type-daml-script-internal-questions-usermanagement-invaliduserid-35585:

**data** `InvalidUserId <type-daml-script-internal-questions-usermanagement-invaliduserid-35585_>`_

  Thrown if text for a user identifier does not conform to the format restriction\.

  .. _constr-daml-script-internal-questions-usermanagement-invaliduserid-47622:

  `InvalidUserId <constr-daml-script-internal-questions-usermanagement-invaliduserid-47622_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - m
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-usermanagement-user-21930:

**data** `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  User\-info record for a user in the user management service\.

  .. _constr-daml-script-internal-questions-usermanagement-user-51383:

  `User <constr-daml-script-internal-questions-usermanagement-user-51383_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -
       * - primaryParty
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         -

.. _type-daml-script-internal-questions-usermanagement-useralreadyexists-98333:

**data** `UserAlreadyExists <type-daml-script-internal-questions-usermanagement-useralreadyexists-98333_>`_

  Thrown if a user to be created already exists\.

  .. _constr-daml-script-internal-questions-usermanagement-useralreadyexists-40670:

  `UserAlreadyExists <constr-daml-script-internal-questions-usermanagement-useralreadyexists-40670_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -

.. _type-daml-script-internal-questions-usermanagement-userid-11123:

**data** `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  Identifier for a user in the user management service\.

  .. _constr-daml-script-internal-questions-usermanagement-userid-52094:

  `UserId <constr-daml-script-internal-questions-usermanagement-userid-52094_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_


.. _type-daml-script-internal-questions-usermanagement-usernotfound-44479:

**data** `UserNotFound <type-daml-script-internal-questions-usermanagement-usernotfound-44479_>`_

  Thrown if a user cannot be located for a given user identifier\.

  .. _constr-daml-script-internal-questions-usermanagement-usernotfound-26338:

  `UserNotFound <constr-daml-script-internal-questions-usermanagement-usernotfound-26338_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - userId
         - `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_
         -

.. _type-daml-script-internal-questions-usermanagement-userright-13475:

**data** `UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_

  The rights of a user\.

  .. _constr-daml-script-internal-questions-usermanagement-participantadmin-36398:

  `ParticipantAdmin <constr-daml-script-internal-questions-usermanagement-participantadmin-36398_>`_


  .. _constr-daml-script-internal-questions-usermanagement-canactas-78256:

  `CanActAs <constr-daml-script-internal-questions-usermanagement-canactas-78256_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_


  .. _constr-daml-script-internal-questions-usermanagement-canreadas-21035:

  `CanReadAs <constr-daml-script-internal-questions-usermanagement-canreadas-21035_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_


  .. _constr-daml-script-internal-questions-usermanagement-canreadasanyparty-13813:

  `CanReadAsAnyParty <constr-daml-script-internal-questions-usermanagement-canreadasanyparty-13813_>`_


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
         - \[`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (\[CommandResult\], `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_)\] \-\> a
         -

.. _type-daml-script-internal-questions-submit-packageid-95921:

**data** `PackageId <type-daml-script-internal-questions-submit-packageid-95921_>`_

  Package\-id newtype for package preference

  .. _constr-daml-script-internal-questions-submit-packageid-38878:

  `PackageId <constr-daml-script-internal-questions-submit-packageid-38878_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_


.. _type-daml-script-internal-questions-submit-submitoptions-56692:

**data** `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

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

  .. _constr-daml-script-internal-questions-submit-submitoptions-37975:

  `SubmitOptions <constr-daml-script-internal-questions-submit-submitoptions-37975_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - soActAs
         - \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - soReadAs
         - \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - soDisclosures
         - \[`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_\]
         -
       * - soPackagePreference
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ \[`PackageId <type-daml-script-internal-questions-submit-packageid-95921_>`_\]
         -
       * - soPrefetchKeys
         - \[`AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]
         -

.. _type-daml-script-internal-questions-submit-error-cryptoerrortype-71749:

**data** `CryptoErrorType <type-daml-script-internal-questions-submit-error-cryptoerrortype-71749_>`_

  Daml Crypto (Secp256k1) related submission errors

  .. _constr-daml-script-internal-questions-submit-error-malformedbyteencoding-79193:

  `MalformedByteEncoding <constr-daml-script-internal-questions-submit-error-malformedbyteencoding-79193_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - value
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
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
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
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
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-submit-error-submiterror-38284:

**data** `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_

  Errors that can be thrown by a command submission via ``trySubmit``

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
         - `NonEmpty <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-NonEmpty-Types.html#type-da-nonempty-types-nonempty-16010>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         - Provided as text, as we do not know the template ID of a contract if the lookup fails
       * - additionalDebuggingInfo
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199>`
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
         - `AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
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
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
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
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-contracthashingerror-75608:

  `ContractHashingError <constr-daml-script-internal-questions-submit-error-contracthashingerror-75608_>`_

    Failed to hash a contract

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - contractId
         - `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
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
         - `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
         -
       * - expectedKey
         - `AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
         -
       * - givenKeyHash
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
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
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
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
         - `AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
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
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyException <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_
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
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
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
         - `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
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
         - `AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_
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
         - `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
         - Any contract Id of the actual contract
       * - expectedTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - actualTemplateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
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
         - `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
         -
       * - templateId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - interfaceId
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
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
         - `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
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

  .. _constr-daml-script-internal-questions-submit-error-noncomparablevalues-97474:

  `NonComparableValues <constr-daml-script-internal-questions-submit-error-noncomparablevalues-97474_>`_

    Attempted to compare values that are not comparable

  .. _constr-daml-script-internal-questions-submit-error-contractidincontractkey-60542:

  `ContractIdInContractKey <constr-daml-script-internal-questions-submit-error-contractidincontractkey-60542_>`_

    Illegal Contract ID found in Contract Key

    (no fields)

  .. _constr-daml-script-internal-questions-submit-error-contractidcomparability-98492:

  `ContractIdComparability <constr-daml-script-internal-questions-submit-error-contractidcomparability-98492_>`_

    Attempted to compare incomparable contract IDs\. You're doing something very wrong\.
    Two contract IDs with the same prefix are incomparable if one of them is local and the other non\-local
    or if one is relative and the other relative or absolute with a different suffix\.

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - globalExistingContractId
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
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
         - `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
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
         - \[`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_\]
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
         - \[`AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\]
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
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-failurestatuserror-13880:

  `FailureStatusError <constr-daml-script-internal-questions-submit-error-failurestatuserror-13880_>`_

    Exception resulting from call to ``failWithStatus``

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - failureStatus
         - `FailureStatus <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Fail.html#type-da-internal-fail-types-failurestatus-69615>`_
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
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
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
         - DevErrorType
         -
       * - devErrorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
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
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  .. _constr-daml-script-internal-questions-submit-error-truncatederror-47926:

  `TruncatedError <constr-daml-script-internal-questions-submit-error-truncatederror-47926_>`_

    One of the above error types where the full exception body did not fit into the response, and was incomplete\.

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

.. _type-daml-script-internal-questions-submit-error-upgradeerrortype-94779:

**data** `UpgradeErrorType <type-daml-script-internal-questions-submit-error-upgradeerrortype-94779_>`_

  SCU related submission errors

  .. _constr-daml-script-internal-questions-submit-error-validationfailed-35370:

  `ValidationFailed <constr-daml-script-internal-questions-submit-error-validationfailed-35370_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - coid
         - `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
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
       * - originalObservers
         - \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - originalKeyOpt
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\])
         -
       * - recomputedSignatories
         - \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - recomputedObservers
         - \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]
         -
       * - recomputedKeyOpt
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_, \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\])
         -

  .. _constr-daml-script-internal-questions-submit-error-translationfailed-4701:

  `TranslationFailed <constr-daml-script-internal-questions-submit-error-translationfailed-4701_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - mCoid
         - `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
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

  .. _constr-daml-script-internal-questions-submit-error-authenticationfailed-3671:

  `AuthenticationFailed <constr-daml-script-internal-questions-submit-error-authenticationfailed-3671_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - coid
         - `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
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
         - `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
         -
       * - argument
         - `AnyTemplate <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anytemplate-63703>`_
         -

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
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - offset
         - `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -

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
         - `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_
         -
       * - choice
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - argument
         - `AnyChoice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anychoice-86490>`_
         -
       * - childEvents
         - \[`TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_\]
         -

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
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - choice
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - offset
         - `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_
         -
       * - child
         - `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t
         -

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

.. _type-daml-script-internal-questions-transactiontree-treeevent-1267:

**data** `TreeEvent <type-daml-script-internal-questions-transactiontree-treeevent-1267_>`_

  .. _constr-daml-script-internal-questions-transactiontree-createdevent-60119:

  `CreatedEvent <constr-daml-script-internal-questions-transactiontree-createdevent-60119_>`_ `Created <type-daml-script-internal-questions-transactiontree-created-98301_>`_


  .. _constr-daml-script-internal-questions-transactiontree-exercisedevent-2627:

  `ExercisedEvent <constr-daml-script-internal-questions-transactiontree-exercisedevent-2627_>`_ `Exercised <type-daml-script-internal-questions-transactiontree-exercised-22057_>`_


.. _type-daml-script-internal-questions-transactiontree-treeindex-21327:

**data** `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t

  .. _constr-daml-script-internal-questions-transactiontree-createdindex-88223:

  `CreatedIndex <constr-daml-script-internal-questions-transactiontree-createdindex-88223_>`_ (`CreatedIndexPayload <type-daml-script-internal-questions-transactiontree-createdindexpayload-52051_>`_ t)


  .. _constr-daml-script-internal-questions-transactiontree-exercisedindex-22399:

  `ExercisedIndex <constr-daml-script-internal-questions-transactiontree-exercisedindex-22399_>`_ (`ExercisedIndexPayload <type-daml-script-internal-questions-transactiontree-exercisedindexpayload-19779_>`_ t)


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
         - `TemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-templatetyperep-33792>`_
         -
       * - contractId
         - `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ ()
         -

.. _type-daml-script-internal-questions-partymanagement-participantname-88190:

**data** `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_

  Participant name for multi\-participant script runs to address a specific participant

  .. _constr-daml-script-internal-questions-partymanagement-participantname-13079:

  `ParticipantName <constr-daml-script-internal-questions-partymanagement-participantname-13079_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - participantName
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-partymanagement-partydetails-4369:

**data** `PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_

  The party details returned by the party management service\.

  .. _constr-daml-script-internal-questions-partymanagement-partydetails-1790:

  `PartyDetails <constr-daml-script-internal-questions-partymanagement-partydetails-1790_>`_

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

.. _type-daml-script-internal-questions-partymanagement-partyidhint-14540:

**data** `PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540_>`_

  A hint to the backing participant what party id to allocate\.
  Must be a valid PartyIdString (as described in @value\.proto@)\.

  .. _constr-daml-script-internal-questions-partymanagement-partyidhint-11617:

  `PartyIdHint <constr-daml-script-internal-questions-partymanagement-partyidhint-11617_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - partyIdHint
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-crypto-text-privatekeyhex-82732:

**type** `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_
  \= `BytesHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  A DER formatted private key to be used for ECDSA message signing

.. _type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395:

**data** `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_

  Secp256k1 key pair generated by ``secp256k1generatekeypair`` for testing\.

  .. _constr-daml-script-internal-questions-crypto-text-secp256k1keypair-60460:

  `Secp256k1KeyPair <constr-daml-script-internal-questions-crypto-text-secp256k1keypair-60460_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - privateKey
         - `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_
         -
       * - publicKey
         - `PublicKeyHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-publickeyhex-51359>`_
         -

.. _type-daml-script-internal-questions-commands-commands-79301:

**data** `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a

  This is used to build up the commands sent as part of ``submit``\.
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
         - \[CommandWithMeta\]
         -
       * - continue
         - \[CommandResult\] \-\> a
         -

.. _type-daml-script-internal-questions-commands-disclosure-40298:

**data** `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_

  Contract disclosures which can be acquired via ``queryDisclosure``

  .. _constr-daml-script-internal-questions-commands-disclosure-14083:

  `Disclosure <constr-daml-script-internal-questions-commands-disclosure-14083_>`_

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

.. _type-daml-script-internal-lowlevel-script-4781:

**data** `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a

  This is the type of A Daml script\. ``Script`` is an instance of ``Action``,
  so you can use ``do`` notation\.

  .. _constr-daml-script-internal-lowlevel-script-73096:

  `Script <constr-daml-script-internal-lowlevel-script-73096_>`_

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
  \: `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  Extract the name\-text from a user identitifer\.

.. _function-daml-script-internal-questions-usermanagement-validateuserid-51917:

`validateUserId <function-daml-script-internal-questions-usermanagement-validateuserid-51917_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_

  Construct a user identifer from text\. May throw InvalidUserId\.

.. _function-daml-script-internal-questions-usermanagement-createuser-37948:

`createUser <function-daml-script-internal-questions-usermanagement-createuser-37948_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ ()

  Create a user with the given rights\. May throw UserAlreadyExists\.

.. _function-daml-script-internal-questions-usermanagement-createuseron-3905:

`createUserOn <function-daml-script-internal-questions-usermanagement-createuseron-3905_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ ()

  Create a user with the given rights on the given participant\. May throw UserAlreadyExists\.

.. _function-daml-script-internal-questions-usermanagement-getuser-5077:

`getUser <function-daml-script-internal-questions-usermanagement-getuser-5077_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  Fetch a user record by user id\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-getuseron-1968:

`getUserOn <function-daml-script-internal-questions-usermanagement-getuseron-1968_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `User <type-daml-script-internal-questions-usermanagement-user-21930_>`_

  Fetch a user record by user id from the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-listallusers-63416:

`listAllUsers <function-daml-script-internal-questions-usermanagement-listallusers-63416_>`_
  \: `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`User <type-daml-script-internal-questions-usermanagement-user-21930_>`_\]

  List all users\. This function may make multiple calls to underlying paginated ledger API\.

.. _function-daml-script-internal-questions-usermanagement-listalluserson-20857:

`listAllUsersOn <function-daml-script-internal-questions-usermanagement-listalluserson-20857_>`_
  \: `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`User <type-daml-script-internal-questions-usermanagement-user-21930_>`_\]

  List all users on the given participant\. This function may make multiple calls to underlying paginated ledger API\.

.. _function-daml-script-internal-questions-usermanagement-grantuserrights-87478:

`grantUserRights <function-daml-script-internal-questions-usermanagement-grantuserrights-87478_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  Grant rights to a user\. Returns the rights that have been newly granted\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-grantuserrightson-91259:

`grantUserRightsOn <function-daml-script-internal-questions-usermanagement-grantuserrightson-91259_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  Grant rights to a user on the given participant\. Returns the rights that have been newly granted\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-revokeuserrights-85325:

`revokeUserRights <function-daml-script-internal-questions-usermanagement-revokeuserrights-85325_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  Revoke rights for a user\. Returns the revoked rights\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-revokeuserrightson-21608:

`revokeUserRightsOn <function-daml-script-internal-questions-usermanagement-revokeuserrightson-21608_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\] \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  Revoke rights for a user on the given participant\. Returns the revoked rights\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-deleteuser-2585:

`deleteUser <function-daml-script-internal-questions-usermanagement-deleteuser-2585_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ ()

  Delete a user\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-deleteuseron-74248:

`deleteUserOn <function-daml-script-internal-questions-usermanagement-deleteuseron-74248_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ ()

  Delete a user on the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-listuserrights-50525:

`listUserRights <function-daml-script-internal-questions-usermanagement-listuserrights-50525_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  List the rights of a user\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-listuserrightson-11796:

`listUserRightsOn <function-daml-script-internal-questions-usermanagement-listuserrightson-11796_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`UserRight <type-daml-script-internal-questions-usermanagement-userright-13475_>`_\]

  List the rights of a user on the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-submituser-29476:

`submitUser <function-daml-script-internal-questions-usermanagement-submituser-29476_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a

  Submit the commands with the actAs and readAs claims granted to a user\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-usermanagement-submituseron-39337:

`submitUserOn <function-daml-script-internal-questions-usermanagement-submituseron-39337_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `UserId <type-daml-script-internal-questions-usermanagement-userid-11123_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a

  Submit the commands with the actAs and readAs claims granted to the user on the given participant\. May throw UserNotFound\.

.. _function-daml-script-internal-questions-time-settime-32330:

`setTime <function-daml-script-internal-questions-time-settime-32330_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Time <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-time-63886>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ ()

  Set the time via the time service\.

  This is only supported in Daml Studio and ``dpm test`` as well as
  when running over the gRPC API against a ledger in static time mode\.

  Note that the ledger time service does not support going backwards in time\.
  However, you can go back in time in Daml Studio\.

.. _function-daml-script-internal-questions-time-sleep-58882:

`sleep <function-daml-script-internal-questions-time-sleep-58882_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `RelTime <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Time.html#type-da-time-types-reltime-23082>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ ()

  Sleep for the given duration\.

  This is primarily useful in tests
  where you repeatedly call ``query`` until a certain state is reached\.

  Note that this will sleep for the same duration in both wall clock and static time mode\.

.. _function-daml-script-internal-questions-time-passtime-50024:

`passTime <function-daml-script-internal-questions-time-passtime-50024_>`_
  \: `RelTime <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Time.html#type-da-time-types-reltime-23082>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ ()

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
  \: `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ parties \=\> parties \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

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
  \: `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ parties \=\> parties \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

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
  \: \[`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_\] \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Provides many Explicit Disclosures to the transaction\.

.. _function-daml-script-internal-questions-submit-disclose-59895:

`disclose <function-daml-script-internal-questions-submit-disclose-59895_>`_
  \: `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_ \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Provides an Explicit Disclosure to the transaction\.

.. _function-daml-script-internal-questions-submit-packagepreference-25445:

`packagePreference <function-daml-script-internal-questions-submit-packagepreference-25445_>`_
  \: \[`PackageId <type-daml-script-internal-questions-submit-packageid-95921_>`_\] \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Provide a package id selection preference for upgrades for a submission

.. _function-daml-script-internal-questions-submit-prefetchkeys-84998:

`prefetchKeys <function-daml-script-internal-questions-submit-prefetchkeys-84998_>`_
  \: \[`AnyContractKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-any-anycontractkey-68193>`_\] \-\> `SubmitOptions <type-daml-script-internal-questions-submit-submitoptions-56692_>`_

  Provide a list of contract keys to prefetch for a submission

.. _function-daml-script-internal-questions-submit-concurrently-75077:

`concurrently <function-daml-script-internal-questions-submit-concurrently-75077_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `ConcurrentSubmits <type-daml-script-internal-questions-submit-concurrentsubmits-82688_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a

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
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script (a, `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_)

  Equivalent to ``submit`` but returns the result and the full transaction tree\.

.. _function-daml-script-internal-questions-submit-trysubmitresultandtree-33682:

`trySubmitResultAndTree <function-daml-script-internal-questions-submit-trysubmitresultandtree-33682_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ (a, `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_))

  Equivalent to ``trySubmit`` but returns the result and the full transaction tree\.

.. _function-daml-script-internal-questions-submit-submitwitherror-52958:

`submitWithError <function-daml-script-internal-questions-submit-submitwitherror-52958_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_

  Equivalent to ``submitMustFail`` but returns the error thrown\.

.. _function-daml-script-internal-questions-submit-submit-5889:

`submit <function-daml-script-internal-questions-submit-submit-5889_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script a

  ``submit p cmds`` submits the commands ``cmds`` as a single transaction
  from party ``p`` and returns the value returned by ``cmds``\.
  The ``options`` field can either be any \"Parties\" like type (See ``IsParties``) or ``SubmitOptions``
  which allows for finer control over parameters of the submission\.

  If the transaction fails, ``submit`` also fails\.

.. _function-daml-script-internal-questions-submit-submitwithoptions-56152:

`submitWithOptions <function-daml-script-internal-questions-submit-submitwithoptions-56152_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script a

  .. warning::
    **DEPRECATED**\:

    | Daml 2\.9 compatibility helper, use 'submit' instead

.. _function-daml-script-internal-questions-submit-submittree-5925:

`submitTree <function-daml-script-internal-questions-submit-submittree-5925_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_

  Equivalent to ``submit`` but returns the full transaction tree\.

.. _function-daml-script-internal-questions-submit-trysubmit-23693:

`trySubmit <function-daml-script-internal-questions-submit-trysubmit-23693_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ a)

  Submit a transaction and receive back either the result, or a ``SubmitError``\.
  In the majority of failures, this will not crash at runtime\.

.. _function-daml-script-internal-questions-submit-trysubmittree-68085:

`trySubmitTree <function-daml-script-internal-questions-submit-trysubmittree-68085_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_)

  Equivalent to ``trySubmit`` but returns the full transaction tree\.

.. _function-daml-script-internal-questions-submit-submitmustfail-63662:

`submitMustFail <function-daml-script-internal-questions-submit-submitmustfail-63662_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script ()

  ``submitMustFail p cmds`` submits the commands ``cmds`` as a single transaction
  from party ``p``\.
  See submitWithOptions for details on the ``options`` field

  It only succeeds if the submitting the transaction fails\.

.. _function-daml-script-internal-questions-submit-submitmustfailwithoptions-20017:

`submitMustFailWithOptions <function-daml-script-internal-questions-submit-submitmustfailwithoptions-20017_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script, `IsSubmitOptions <class-daml-script-internal-questions-submit-issubmitoptions-64211_>`_ options) \=\> options \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script ()

  .. warning::
    **DEPRECATED**\:

    | Daml 2\.9 compatibility helper, use 'submitMustFail' instead

.. _function-daml-script-internal-questions-submit-submitmulti-45107:

`submitMulti <function-daml-script-internal-questions-submit-submitmulti-45107_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script a

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
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script ()

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``submitMustFail``, ``actAs`` and ``readAs`` separately

  ``submitMultiMustFail actAs readAs cmds`` behaves like ``submitMulti actAs readAs cmds``
  but fails when ``submitMulti`` succeeds and the other way around\.

  Note\: This behaviour can be achieved using ``submitMustFail (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-submittreemulti-4879:

`submitTreeMulti <function-daml-script-internal-questions-submit-submittreemulti-4879_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``submitTree``, ``actAs`` and ``readAs`` separately

  Equivalent to ``submitMulti`` but returns the full transaction tree\.

  Note\: This behaviour can be achieved using ``submitTree (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-trysubmitmulti-31939:

`trySubmitMulti <function-daml-script-internal-questions-submit-trysubmitmulti-31939_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `ScriptSubmit <class-daml-script-internal-questions-submit-scriptsubmit-55101_>`_ script) \=\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> script (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ a)

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``trySubmit``, ``actAs`` and ``readAs`` separately

  Alternate version of ``trySubmit`` that allows specifying the actAs and readAs parties\.

  Note\: This behaviour can be achieved using ``trySubmit (actAs actors <> readAs readers) cmds``
  and is only provided for backwards compatibility\.

.. _function-daml-script-internal-questions-submit-trysubmitconcurrently-11443:

`trySubmitConcurrently <function-daml-script-internal-questions-submit-trysubmitconcurrently-11443_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> \[`Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a\] \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `SubmitError <type-daml-script-internal-questions-submit-error-submiterror-38284_>`_ a\]

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``concurrent`` and ``trySubmit`` separately

.. _function-daml-script-internal-questions-submit-submitwithdisclosures-50120:

`submitWithDisclosures <function-daml-script-internal-questions-submit-submitwithdisclosures-50120_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> \[`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``trySubmit`` and ``disclosures`` separately

.. _function-daml-script-internal-questions-submit-submitwithdisclosuresmustfail-28475:

`submitWithDisclosuresMustFail <function-daml-script-internal-questions-submit-submitwithdisclosuresmustfail-28475_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_ \-\> \[`Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_\] \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ ()

  .. warning::
    **DEPRECATED**\:

    | Legacy API, use ``submitMustFail`` and ``disclosures`` separately

.. _function-daml-script-internal-questions-transactiontree-fromtree-1340:

`fromTree <function-daml-script-internal-questions-transactiontree-fromtree-1340_>`_
  \: `Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t \=\> `TransactionTree <type-daml-script-internal-questions-transactiontree-transactiontree-91781_>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t \-\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t

  Finds the contract id of an event within a tree given a tree index
  Tree indices are created using the ``created(N)`` and ``exercised(N)`` builders
  which allow building \"paths\" within a transaction to a create node
  For example, ``exercisedN @MyTemplate1 "MyChoice" 2 $ createdN @MyTemplate2 1``
  would find the ``ContractId MyTemplate2`` of the second (0 index) create event under
  the 3rd exercise event of ``MyChoice`` from ``MyTemplate1``

.. _function-daml-script-internal-questions-transactiontree-created-56097:

`created <function-daml-script-internal-questions-transactiontree-created-56097_>`_
  \: `HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t

  Index for the first create event of a given template
  e\.g\. ``created @MyTemplate``

.. _function-daml-script-internal-questions-transactiontree-createdn-71930:

`createdN <function-daml-script-internal-questions-transactiontree-createdn-71930_>`_
  \: `HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t

  Index for the Nth create event of a given template
  e\.g\. ``createdN 2 @MyTemplate``
  ``created = createdN 0``

.. _function-daml-script-internal-questions-transactiontree-exercised-13349:

`exercised <function-daml-script-internal-questions-transactiontree-exercised-13349_>`_
  \: `HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t' \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t'

  Index for the first exercise of a given choice on a given template
  e\.g\. ``exercised @MyTemplate "MyChoice"``

.. _function-daml-script-internal-questions-transactiontree-exercisedn-70910:

`exercisedN <function-daml-script-internal-questions-transactiontree-exercisedn-70910_>`_
  \: `HasTemplateTypeRep <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hastemplatetyperep-24134>`_ t \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `Int <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-int-37261>`_ \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t' \-\> `TreeIndex <type-daml-script-internal-questions-transactiontree-treeindex-21327_>`_ t'

  Index for the Nth exercise of a given choice on a given template
  e\.g\. ``exercisedN @MyTemplate "MyChoice" 2``
  ``exercised c = exercisedN c 0``

.. _function-daml-script-internal-questions-util-fromanycontractid-11435:

`fromAnyContractId <function-daml-script-internal-questions-util-fromanycontractid-11435_>`_
  \: `Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t \=\> `AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399_>`_ \-\> `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t)

.. _function-daml-script-internal-questions-query-query-55941:

`query <function-daml-script-internal-questions-query-query-55941_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[(`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, t)\]

  Query the set of active contracts of the template
  that are visible to the given party\.

.. _function-daml-script-internal-questions-query-queryfilter-99157:

`queryFilter <function-daml-script-internal-questions-query-queryfilter-99157_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ c, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ c, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> (c \-\> `Bool <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_) \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[(`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ c, c)\]

  Query the set of active contracts of the template
  that are visible to the given party and match the given predicate\.

.. _function-daml-script-internal-questions-query-querycontractid-24166:

`queryContractId <function-daml-script-internal-questions-query-querycontractid-24166_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ t)

  Query for the contract with the given contract id\.

  Returns ``None`` if there is no active contract the party is a stakeholder on\.

  WARNING\: Over the gRPC backend this performs a linear search over all contracts of
  the same type, so only use this if the number of active contracts is small\.

  This is semantically equivalent to calling ``query``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-query-querydisclosure-12000:

`queryDisclosure <function-daml-script-internal-questions-query-querydisclosure-12000_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Disclosure <type-daml-script-internal-questions-commands-disclosure-40298_>`_)

  Queries a Disclosure for a given ContractId\. Same performance caveats apply as to ``queryContractId``\.

.. _function-daml-script-internal-questions-query-queryinterface-52085:

`queryInterface <function-daml-script-internal-questions-query-queryinterface-52085_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ i, `HasInterfaceView <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492>`_ i v, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[(`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ i, `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ v)\]

  Query the set of active contract views for an interface
  that are visible to the given party\.
  If the view function fails for a given contract id, The ``Optional v`` will be ``None``\.

.. _function-daml-script-internal-questions-query-queryinterfacecontractid-18438:

`queryInterfaceContractId <function-daml-script-internal-questions-query-queryinterfacecontractid-18438_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ i, `HasInterfaceView <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-interface-hasinterfaceview-4492>`_ i v, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p, `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_) \=\> p \-\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ i \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ v)

  Query for the contract view with the given contract id\.

  Returns ``None`` if there is no active contract the party is a stakeholder on\.

  Returns ``None`` if the view function fails for the given contract id\.

  WARNING\: Over the gRPC backend this performs a linear search over all contracts of
  the same type, so only use this if the number of active contracts is small\.

  This is semantically equivalent to calling ``queryInterface``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-query-querycontractkey-51277:

`queryContractKey <function-daml-script-internal-questions-query-querycontractkey-51277_>`_
  \: (`HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_, `TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `IsParties <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-isparties-53750>`_ p) \=\> p \-\> k \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, t))

  Returns ``None`` if there is no active contract with the given key that
  the party is a stakeholder on\.

  WARNING\: Over the gRPC backend this performs a linear search over all contracts of
  the same type, so only use this if the number of active contracts is small\.

  This is semantically equivalent to calling ``query``
  and filtering on the client side\.

.. _function-daml-script-internal-questions-partymanagement-allocateparty-4749:

`allocateParty <function-daml-script-internal-questions-partymanagement-allocateparty-4749_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name
  using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-allocatepartywithhint-96426:

`allocatePartyWithHint <function-daml-script-internal-questions-partymanagement-allocatepartywithhint-96426_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  .. warning::
    **DEPRECATED**\:

    | Daml 3\.3 compatibility helper, use 'allocatePartyByHint' instead of 'allocatePartyWithHint'

.. _function-daml-script-internal-questions-partymanagement-allocatepartybyhint-55067:

`allocatePartyByHint <function-daml-script-internal-questions-partymanagement-allocatepartybyhint-55067_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given id hint
  using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-allocatepartyon-59020:

`allocatePartyOn <function-daml-script-internal-questions-partymanagement-allocatepartyon-59020_>`_
  \: `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name
  on the specified participant using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-allocatepartywithhinton-11859:

`allocatePartyWithHintOn <function-daml-script-internal-questions-partymanagement-allocatepartywithhinton-11859_>`_
  \: `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  .. warning::
    **DEPRECATED**\:

    | Daml 3\.3 compatibility helper, use 'allocatePartyByHintOn' instead of 'allocatePartyWithHintOn'

.. _function-daml-script-internal-questions-partymanagement-allocatepartybyhinton-5218:

`allocatePartyByHintOn <function-daml-script-internal-questions-partymanagement-allocatepartybyhinton-5218_>`_
  \: `PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given id hint
  on the specified participant using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-listknownparties-55540:

`listKnownParties <function-daml-script-internal-questions-partymanagement-listknownparties-55540_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_\]

  List the parties known to the default participant\.

.. _function-daml-script-internal-questions-partymanagement-listknownpartieson-55333:

`listKnownPartiesOn <function-daml-script-internal-questions-partymanagement-listknownpartieson-55333_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ \[`PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_\]

  List the parties known to the given participant\.

.. _function-daml-script-internal-questions-exceptions-trytoeither-58773:

`tryToEither <function-daml-script-internal-questions-exceptions-trytoeither-58773_>`_
  \: (() \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ t) \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `AnyException <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_ t)

  Named version of the ``try catch`` behaviour of Daml\-Script\.
  Note that this is no more powerful than ``try catch`` in daml\-script, and will not catch exceptions in submissions\.
  (Use ``trySubmit`` for this)
  Input computation is deferred to catch pure exceptions

.. _function-daml-script-internal-questions-exceptions-tryfailurestatus-576:

`tryFailureStatus <function-daml-script-internal-questions-exceptions-tryfailurestatus-576_>`_
  \: `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `FailureStatus <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Fail.html#type-da-internal-fail-types-failurestatus-69615>`_ a)

  Runs a script for a result\. If it fails either by Daml Exceptions or ``failWithStatus``, returns the
  ``FailureStatus`` that a Canton Ledger would return\.

.. _function-daml-script-internal-questions-crypto-text-secp256k1signwithecdsaonly-99207:

`secp256k1signWithEcdsaOnly <function-daml-script-internal-questions-crypto-text-secp256k1signwithecdsaonly-99207_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_ \-\> `BytesHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `BytesHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  Using a DER formatted private key (encoded as a hex string) use Secp256k1 to sign a hex encoded string message\.

  Note that this implementation uses a random source with a fixed PRNG and seed, ensuring it behaves deterministically during testing\.

  For example, CCTP attestation services may be mocked in daml\-script code\.

.. _function-daml-script-internal-questions-crypto-text-secp256k1sign-72886:

`secp256k1sign <function-daml-script-internal-questions-crypto-text-secp256k1sign-72886_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `PrivateKeyHex <type-daml-script-internal-questions-crypto-text-privatekeyhex-82732_>`_ \-\> `BytesHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_ \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `BytesHex <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Crypto-Text.html#type-da-crypto-text-byteshex-47880>`_

  Using a DER formatted private key (encoded as a hex string) use Secp256k1 to sign a SHA256 digest of a hex encoded string message\.

  Note that this implementation uses a random source with a fixed PRNG and seed, ensuring it behaves deterministically during testing\.

  For example, CCTP attestation services may be mocked in daml\-script code\.

.. _function-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-90200:

`secp256k1generatekeypair <function-daml-script-internal-questions-crypto-text-secp256k1generatekeypair-90200_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ `Secp256k1KeyPair <type-daml-script-internal-questions-crypto-text-secp256k1keypair-9395_>`_

  Generate DER formatted Secp256k1 public/private key pairs\.

.. _function-daml-script-internal-questions-commands-createcmd-46830:

`createCmd <function-daml-script-internal-questions-commands-createcmd-46830_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t)

  Create a contract of the given template\.

.. _function-daml-script-internal-questions-commands-exercisecmd-7438:

`exerciseCmd <function-daml-script-internal-questions-commands-exercisecmd-7438_>`_
  \: `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r \=\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Exercise a choice on the given contract\.

.. _function-daml-script-internal-questions-commands-exercisebykeycmd-80697:

`exerciseByKeyCmd <function-daml-script-internal-questions-commands-exercisebykeycmd-80697_>`_
  \: (`TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r) \=\> k \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Exercise a choice on the contract with the given key\.

.. _function-daml-script-internal-questions-commands-createandexercisewithcidcmd-21289:

`createAndExerciseWithCidCmd <function-daml-script-internal-questions-commands-createandexercisewithcidcmd-21289_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, r)

  Create a contract and exercise a choice on it in the same transaction, returns the created ContractId, and the choice result\.

.. _function-daml-script-internal-questions-commands-createandexercisecmd-8600:

`createAndExerciseCmd <function-daml-script-internal-questions-commands-createandexercisecmd-8600_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Create a contract and exercise a choice on it in the same transaction, returns only the choice result\.

.. _function-daml-script-internal-questions-commands-createexactcmd-86998:

`createExactCmd <function-daml-script-internal-questions-commands-createexactcmd-86998_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t)

  Create a contract of the given template, using the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-exerciseexactcmd-18398:

`exerciseExactCmd <function-daml-script-internal-questions-commands-exerciseexactcmd-18398_>`_
  \: `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r \=\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Exercise a choice on the given contract, using the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-exercisebykeyexactcmd-4555:

`exerciseByKeyExactCmd <function-daml-script-internal-questions-commands-exercisebykeyexactcmd-4555_>`_
  \: (`TemplateKey <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-templatekey-95200>`_ t k, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r) \=\> k \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Exercise a choice on the contract with the given key, using the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-createandexercisewithcidexactcmd-15363:

`createAndExerciseWithCidExactCmd <function-daml-script-internal-questions-commands-createandexercisewithcidexactcmd-15363_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ (`ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t, r)

  Create a contract and exercise a choice on it in the same transaction, returns the created ContractId, and the choice result\.
  Uses the exact package ID of the template given \- upgrades are disabled\.

.. _function-daml-script-internal-questions-commands-createandexerciseexactcmd-54956:

`createAndExerciseExactCmd <function-daml-script-internal-questions-commands-createandexerciseexactcmd-54956_>`_
  \: (`Template <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-template-31804>`_ t, `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t c r, `HasEnsure <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-da-internal-template-functions-hasensure-18132>`_ t) \=\> t \-\> c \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ r

  Create a contract and exercise a choice on it in the same transaction, returns only the choice result\.

.. _function-daml-script-internal-questions-commands-archivecmd-47203:

`archiveCmd <function-daml-script-internal-questions-commands-archivecmd-47203_>`_
  \: `Choice <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-functions-choice-82157>`_ t `Archive <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-template-archive-15178>`_ () \=\> `ContractId <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-contractid-95282>`_ t \-\> `Commands <type-daml-script-internal-questions-commands-commands-79301_>`_ ()

  Archive the given contract\.

  ``archiveCmd cid`` is equivalent to ``exerciseCmd cid Archive``\.

.. _function-daml-script-internal-lowlevel-script-65113:

`script <function-daml-script-internal-lowlevel-script-65113_>`_
  \: `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a \-\> `Script <type-daml-script-internal-lowlevel-script-4781_>`_ a

  Convenience helper to declare you are writing a Script\.

  This is only useful for readability and to improve type inference\.
  Any expression of type ``Script a`` is a valid script regardless of whether
  it is implemented using ``script`` or not\.

