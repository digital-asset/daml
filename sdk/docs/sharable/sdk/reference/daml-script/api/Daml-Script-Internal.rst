.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-18793:

Daml.Script.Internal
====================

Contains all Internal and Alpha functionality provided by Daml Script

Data Types
----------

.. _type-daml-script-internal-questions-testing-commandname-12991:

**data** `CommandName <type-daml-script-internal-questions-testing-commandname-12991_>`_

  Name of the Daml Script Command (or Question) that failed

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

.. _type-daml-script-internal-questions-testing-errorclassname-49861:

**data** `ErrorClassName <type-daml-script-internal-questions-testing-errorclassname-49861_>`_

  Scala class name of the exception thrown

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

.. _type-daml-script-internal-questions-testing-errormessage-78991:

**data** `ErrorMessage <type-daml-script-internal-questions-testing-errormessage-78991_>`_

  Result of the ``getMessage`` method on the Scala exception

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

.. _type-daml-script-internal-questions-testing-failedcmd-88074:

**data** `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_

  Daml type representing a Scala exception thrown during script interpretation\.
  Used for internal testing of the Daml Script library\.

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

.. _type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199:

**data** `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_

  Additional debugging information provided only by IDE Ledger

.. _type-daml-script-internal-questions-packages-packagename-68696:

**data** `PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_

  Used for vetting and unvetting packages

  .. _constr-daml-script-internal-questions-packages-packagename-3807:

  `PackageName <constr-daml-script-internal-questions-packages-packagename-3807_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - name
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - version
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

Functions
---------

.. _function-daml-script-internal-questions-testing-trycommands-17332:

`tryCommands <function-daml-script-internal-questions-testing-trycommands-17332_>`_
  \: :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` (`Either <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-types-either-56020>`_ `FailedCmd <type-daml-script-internal-questions-testing-failedcmd-88074_>`_ a)

  Internal testing tool that allows us to catch FailedCmds in the daml language

.. _function-daml-script-internal-questions-testing-liftfailedcommandtofailurestatus-62416:

`liftFailedCommandToFailureStatus <function-daml-script-internal-questions-testing-liftfailedcommandtofailurestatus-62416_>`_
  \: :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` a

  Runs a script and lifts FailedCmd scala exceptions into a FailureStatus, which can be caught via tryFailureStatus

.. _function-daml-script-internal-questions-submit-error-isnotactive-40539:

`isNotActive <function-daml-script-internal-questions-submit-error-isnotactive-40539_>`_
  \: `ContractNotFoundAdditionalInfo <type-daml-script-internal-questions-submit-error-contractnotfoundadditionalinfo-6199_>`_ \-\> `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ :ref:`AnyContractId <type-daml-script-internal-questions-util-anycontractid-11399>`

  Exacts nonactive contract ID from ContractNotFoundAdditionalInfo

.. _function-daml-script-internal-questions-packages-vetpackages-16211:

`vetPackages <function-daml-script-internal-questions-packages-vetpackages-16211_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Vet a set of packages on all participants\.
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-vetpackagesonparticipant-8324:

`vetPackagesOnParticipant <function-daml-script-internal-questions-packages-vetpackagesonparticipant-8324_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Vet a set of packages on a single participant\.
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-unvetpackages-80050:

`unvetPackages <function-daml-script-internal-questions-packages-unvetpackages-80050_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Unvet a set of packages on all participants\.
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-unvetpackagesonparticipant-47459:

`unvetPackagesOnParticipant <function-daml-script-internal-questions-packages-unvetpackagesonparticipant-47459_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\] \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` ()

  Unvet a set of packages on a single participant\.
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-listvettedpackages-3001:

`listVettedPackages <function-daml-script-internal-questions-packages-listvettedpackages-3001_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  Lists the vetted packages on the default participant
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-packages-listallpackages-50063:

`listAllPackages <function-daml-script-internal-questions-packages-listallpackages-50063_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`PackageName <type-daml-script-internal-questions-packages-packagename-68696_>`_\]

  Lists all packages (vetted and unvetted) on the default participant
  Note that the Admin API port must be provided when using this with a Canton Ledger
  Use ``--admin-port`` with the ``daml script`` CLI tool\.

.. _function-daml-script-internal-questions-partymanagement-allocatereplicatedpartyon-96671:

`allocateReplicatedPartyOn <function-daml-script-internal-questions-partymanagement-allocatereplicatedpartyon-96671_>`_
  \: `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> \[:ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>`\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name on the specified main participant using the party management service
  and replicates it to the specified (possibly empty) list of additional participants\. Commands submitted by the
  allocated party will be routed to the main participant\.

.. _function-daml-script-internal-questions-partymanagement-allocatereplicatedpartywithhinton-30144:

`allocateReplicatedPartyWithHintOn <function-daml-script-internal-questions-partymanagement-allocatereplicatedpartywithhinton-30144_>`_
  \: `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> :ref:`PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540>` \-\> :ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>` \-\> \[:ref:`ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190>`\] \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name and id hint on the specified main participant using the party
  management service and replicates it to the specified (possibly empty) list of additional participants\. Commands
  submitted by the allocated party will be routed to the main participant\.

.. _function-daml-script-internal-questions-exceptions-throwanyexception-70957:

`throwAnyException <function-daml-script-internal-questions-exceptions-throwanyexception-70957_>`_
  \: `AnyException <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-anyexception-7004>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` t

  Throws an ``AnyException``, note that this function discards the stacktrace

