.. Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-18793:

Daml.Script.Internal
====================

Contains all Internal and Alpha functionality provided by Daml Script\.
Use these with care\. No stability guarantees are given for them across SDK upgrades\.

Data Types
----------

.. _type-daml-script-internal-questions-testing-stable-failedcmd-failedcmd-6517:

**data** `FailedCmd <type-daml-script-internal-questions-testing-stable-failedcmd-failedcmd-6517_>`_

  Daml type representing a Scala exception thrown during script interpretation\.
  Used for internal testing of the Daml Script library\.

  .. _constr-daml-script-internal-questions-testing-stable-failedcmd-failedcmd-82606:

  `FailedCmd <constr-daml-script-internal-questions-testing-stable-failedcmd-failedcmd-82606_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - commandName
         - `CommandName <type-daml-script-internal-questions-testing-stable-commandname-commandname-51757_>`_
         -
       * - errorClassName
         - `ErrorClassName <type-daml-script-internal-questions-testing-stable-errorclassname-errorclassname-12295_>`_
         -
       * - errorMessage
         - `ErrorMessage <type-daml-script-internal-questions-testing-stable-errormessage-errormessage-8491_>`_
         -

.. _type-daml-script-internal-questions-testing-stable-errormessage-errormessage-8491:

**data** `ErrorMessage <type-daml-script-internal-questions-testing-stable-errormessage-errormessage-8491_>`_

  Result of the ``getMessage`` method on the Scala exception

  .. _constr-daml-script-internal-questions-testing-stable-errormessage-errormessage-18056:

  `ErrorMessage <constr-daml-script-internal-questions-testing-stable-errormessage-errormessage-18056_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getErrorMessage
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-testing-stable-errorclassname-errorclassname-12295:

**data** `ErrorClassName <type-daml-script-internal-questions-testing-stable-errorclassname-errorclassname-12295_>`_

  Scala class name of the exception thrown

  .. _constr-daml-script-internal-questions-testing-stable-errorclassname-errorclassname-40980:

  `ErrorClassName <constr-daml-script-internal-questions-testing-stable-errorclassname-errorclassname-40980_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getErrorClassName
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-testing-stable-commandname-commandname-51757:

**data** `CommandName <type-daml-script-internal-questions-testing-stable-commandname-commandname-51757_>`_

  Name of the Daml Script Command (or Question) that failed

  .. _constr-daml-script-internal-questions-testing-stable-commandname-commandname-53502:

  `CommandName <constr-daml-script-internal-questions-testing-stable-commandname-commandname-53502_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - getCommandName
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

.. _type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199:

**data** `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_

  Additional debugging information provided only by IDE Ledger

  **instance** `Show <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"actAs\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"additionalDebuggingInfo\" :ref:`ContractNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_)

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"additionalInfoCid\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"effectiveAt\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"observers\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `GetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"readAs\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"actAs\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"additionalDebuggingInfo\" :ref:`ContractNotFoundSubmitError <type-daml-script-internal-questions-submit-error-contractnotfoundsubmiterror-39189>` (`Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_)

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"additionalInfoCid\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"effectiveAt\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"observers\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

  **instance** `SetField <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"readAs\" `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \[`Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_\]

.. _type-daml-script-internal-questions-packages-stable-packagename-packagename-80649:

**data** `PackageName <type-daml-script-internal-questions-packages-stable-packagename-packagename-80649_>`_

  Used for vetting and unvetting packages

  .. _constr-daml-script-internal-questions-packages-stable-packagename-packagename-65200:

  `PackageName <constr-daml-script-internal-questions-packages-stable-packagename-packagename-65200_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - name
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - version
         - `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

Functions
---------

.. _function-daml-script-internal-questions-testing-trycommands-17332:

`tryCommands <function-daml-script-internal-questions-testing-trycommands-17332_>`_
  \: :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` (`Either <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `FailedCmd <type-daml-script-internal-questions-testing-stable-failedcmd-failedcmd-6517_>`_ a)

  Internal testing tool that allows us to catch FailedCmds in the daml language

.. _function-daml-script-internal-questions-testing-liftfailedcommandtofailurestatus-62416:

`liftFailedCommandToFailureStatus <function-daml-script-internal-questions-testing-liftfailedcommandtofailurestatus-62416_>`_
  \: :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` a

  Runs a script and lifts FailedCmd scala exceptions into a FailureStatus, which can be caught via tryFailureStatus

.. _function-daml-script-internal-questions-submit-error-isnotactive-40539:

`isNotActive <function-daml-script-internal-questions-submit-error-isnotactive-40539_>`_
  \: `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \-\> `Optional <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-stable-anycontractid-anycontractid-68288>`

  Exacts nonactive contract ID from ContractNotFoundAdditionalInfo

.. _function-daml-script-internal-questions-packages-vetpackages-16211:

`vetPackages <function-daml-script-internal-questions-packages-vetpackages-16211_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-stable-packagename-packagename-80649_>`_\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` ()

  Vet a set of packages on all participants\.
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-vetpackagesonparticipant-8324:

`vetPackagesOnParticipant <function-daml-script-internal-questions-packages-vetpackagesonparticipant-8324_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-stable-packagename-packagename-80649_>`_\] \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` ()

  Vet a set of packages on a single participant\.
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-unvetpackages-80050:

`unvetPackages <function-daml-script-internal-questions-packages-unvetpackages-80050_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-stable-packagename-packagename-80649_>`_\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` ()

  Unvet a set of packages on all participants\.
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-unvetpackagesonparticipant-47459:

`unvetPackagesOnParticipant <function-daml-script-internal-questions-packages-unvetpackagesonparticipant-47459_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-stable-packagename-packagename-80649_>`_\] \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` ()

  Unvet a set of packages on a single participant\.
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-listvettedpackages-3001:

`listVettedPackages <function-daml-script-internal-questions-packages-listvettedpackages-3001_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` \[`PackageName <type-daml-script-internal-questions-packages-stable-packagename-packagename-80649_>`_\]

  Lists the vetted packages on the default participant
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-listallpackages-50063:

`listAllPackages <function-daml-script-internal-questions-packages-listallpackages-50063_>`_
  \: `HasCallStack <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` \[`PackageName <type-daml-script-internal-questions-packages-stable-packagename-packagename-80649_>`_\]

  Lists all packages (vetted and unvetted) on the default participant
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-partymanagement-allocatereplicatedpartyon-96671:

`allocateReplicatedPartyOn <function-daml-script-internal-questions-partymanagement-allocatereplicatedpartyon-96671_>`_
  \: `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562>` \-\> \[:ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562>`\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name on the specified main participant using the party management service
  and replicates it to the specified (possibly empty) list of additional participants\. Commands submitted by the
  allocated party will be routed to the main participant\.

.. _function-daml-script-internal-questions-partymanagement-allocatereplicatedpartywithhinton-30144:

`allocateReplicatedPartyWithHintOn <function-daml-script-internal-questions-partymanagement-allocatereplicatedpartywithhinton-30144_>`_
  \: `Text <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> :ref:`PartyIdHint <type-daml-script-internal-questions-partymanagement-stable-partyidhint-partyidhint-41530>` \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562>` \-\> \[:ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-stable-participantname-participantname-29562>`\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` `Party <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name and id hint on the specified main participant using the party
  management service and replicates it to the specified (possibly empty) list of additional participants\. Commands
  submitted by the allocated party will be routed to the main participant\.

.. _function-daml-script-internal-questions-exceptions-throwanyexception-70957:

`throwAnyException <function-daml-script-internal-questions-exceptions-throwanyexception-70957_>`_
  \: `AnyException <https://docs.digitalasset.com/build/3.4/reference/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-stable-script-script-12809>` t

  Throws an ``AnyException``, note that this function discards the stacktrace

