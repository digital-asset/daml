.. Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _module-daml-script-internal-questions-partymanagement-97209:

Daml.Script.Internal.Questions.PartyManagement
==============================================

Data Types
----------

.. _type-daml-script-internal-questions-partymanagement-allocateparty-41025:

**data** `AllocateParty <type-daml-script-internal-questions-partymanagement-allocateparty-41025_>`_

  .. _constr-daml-script-internal-questions-partymanagement-allocateparty-9792:

  `AllocateParty <constr-daml-script-internal-questions-partymanagement-allocateparty-9792_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - requestedName
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - idHint
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `AllocateParty <type-daml-script-internal-questions-partymanagement-allocateparty-41025_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"idHint\" `AllocateParty <type-daml-script-internal-questions-partymanagement-allocateparty-41025_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `AllocateParty <type-daml-script-internal-questions-partymanagement-allocateparty-41025_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"requestedName\" `AllocateParty <type-daml-script-internal-questions-partymanagement-allocateparty-41025_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"idHint\" `AllocateParty <type-daml-script-internal-questions-partymanagement-allocateparty-41025_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `AllocateParty <type-daml-script-internal-questions-partymanagement-allocateparty-41025_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"requestedName\" `AllocateParty <type-daml-script-internal-questions-partymanagement-allocateparty-41025_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

.. _type-daml-script-internal-questions-partymanagement-listknownparties-97656:

**data** `ListKnownParties <type-daml-script-internal-questions-partymanagement-listknownparties-97656_>`_

  .. _constr-daml-script-internal-questions-partymanagement-listknownparties-85755:

  `ListKnownParties <constr-daml-script-internal-questions-partymanagement-listknownparties-85755_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - participant
         - `Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListKnownParties <type-daml-script-internal-questions-partymanagement-listknownparties-97656_>`_ \[`PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_\]

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participant\" `ListKnownParties <type-daml-script-internal-questions-partymanagement-listknownparties-97656_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participant\" `ListKnownParties <type-daml-script-internal-questions-partymanagement-listknownparties-97656_>`_ (`Optional <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-prelude-optional-37153>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_)

.. _type-daml-script-internal-questions-partymanagement-participantname-88190:

**data** `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_

  .. _constr-daml-script-internal-questions-partymanagement-participantname-13079:

  `ParticipantName <constr-daml-script-internal-questions-partymanagement-participantname-13079_>`_

    .. list-table::
       :widths: 15 10 30
       :header-rows: 1

       * - Field
         - Type
         - Description
       * - participantName
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"participantName\" `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"participantName\" `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

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
         - `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_
         - Party id
       * - isLocal
         - `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_
         - True if party is hosted by the backing participant\.

  **instance** :ref:`IsQuestion <class-daml-script-internal-lowlevel-isquestion-79227>` `ListKnownParties <type-daml-script-internal-questions-partymanagement-listknownparties-97656_>`_ \[`PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_\]

  **instance** `Eq <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-eq-22713>`_ `PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_

  **instance** `Ord <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-classes-ord-6395>`_ `PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_

  **instance** `Show <https://docs.daml.com/daml/stdlib/Prelude.html#class-ghc-show-show-65360>`_ `PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"isLocal\" `PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_ `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"party\" `PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"isLocal\" `PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_ `Bool <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-bool-66265>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"party\" `PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_ `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

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
         - `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_
         -

  **instance** `GetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-getfield-53979>`_ \"partyIdHint\" `PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

  **instance** `SetField <https://docs.daml.com/daml/stdlib/DA-Record.html#class-da-internal-record-setfield-4311>`_ \"partyIdHint\" `PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540_>`_ `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_

Functions
---------

.. _function-daml-script-internal-questions-partymanagement-allocateparty-4749:

`allocateParty <function-daml-script-internal-questions-partymanagement-allocateparty-4749_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name
  using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-allocatepartywithhint-96426:

`allocatePartyWithHint <function-daml-script-internal-questions-partymanagement-allocatepartywithhint-96426_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540_>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name and id hint
  using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-allocatepartyon-59020:

`allocatePartyOn <function-daml-script-internal-questions-partymanagement-allocatepartyon-59020_>`_
  \: `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name
  on the specified participant using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-allocatepartywithhinton-11859:

`allocatePartyWithHintOn <function-daml-script-internal-questions-partymanagement-allocatepartywithhinton-11859_>`_
  \: `Text <https://docs.daml.com/daml/stdlib/Prelude.html#type-ghc-types-text-51952>`_ \-\> `PartyIdHint <type-daml-script-internal-questions-partymanagement-partyidhint-14540_>`_ \-\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` `Party <https://docs.daml.com/daml/stdlib/Prelude.html#type-da-internal-lf-party-57932>`_

  Allocate a party with the given display name and id hint
  on the specified participant using the party management service\.

.. _function-daml-script-internal-questions-partymanagement-listknownparties-55540:

`listKnownParties <function-daml-script-internal-questions-partymanagement-listknownparties-55540_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_\]

  List the parties known to the default participant\.

.. _function-daml-script-internal-questions-partymanagement-listknownpartieson-55333:

`listKnownPartiesOn <function-daml-script-internal-questions-partymanagement-listknownpartieson-55333_>`_
  \: `HasCallStack <https://docs.daml.com/daml/stdlib/DA-Stack.html#type-ghc-stack-types-hascallstack-63713>`_ \=\> `ParticipantName <type-daml-script-internal-questions-partymanagement-participantname-88190_>`_ \-\> :ref:`Script <type-daml-script-internal-lowlevel-script-4781>` \[`PartyDetails <type-daml-script-internal-questions-partymanagement-partydetails-4369_>`_\]

  List the parties known to the given participant\.

