.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

The Multiple Party Agreement Pattern
####################################

The Multiple Party Agreement pattern uses a Pending contract as a wrapper for the Agreement contract. Any one of the signatory parties can kick off the workflow by creating a Pending contract on the ledger, filling in themselves in all the signatory fields. The Agreement contract is not created on the ledger until all parties have agreed to the Pending contract, and replaced the initiator's signature with their own.

Motivation
**********

The :doc:`initaccept` shows how to create bilateral agreements in Daml. However, a project or a workflow often requires more than two parties to reach a consensus and put their signatures on a multi-party contract. For example, in a large construction project, there are at least three major stakeholders: Owner, Architect and Builder. All three parties need to establish agreement on key responsibilities and project success criteria before starting the construction.

If such an agreement were modeled as three separate bilateral agreements, no party could be sure if there are conflicts between their two contracts and the third contract between their partners. If the :doc:`initaccept` were used to collect three signatures on a multi-party agreement, unnecessary restrictions would be put on the order of consensus and a number of additional contract templates would be needed as the intermediate steps. Both solution are suboptimal.

Following the Multiple Party Agreement pattern, it is easy to write an agreement contract with multiple signatories and have each party accept explicitly.

Implementation
**************

Agreement contract
  The *Agreement* contract represents the final agreement among a group of stakeholders. Its content can vary per business case, but in this pattern, it always has multiple signatories.

  .. literalinclude:: daml/MultiplePartyAgreement.daml
    :language: daml
    :start-after: -- start snippet: agreement template
    :end-before: -- end snippet: agreement template

Pending contract
    The *Pending* contract needs to contain the contents of the proposed *Agreement* contract, as a parameter. This is so that parties know what they are agreeing to, and also so that when all parties have signed, the *Agreement* contract can be created.

    The *Pending* contract has a list of parties who have signed it, and a list of parties who have yet to sign it. If you add these lists together, it has to be the same set of parties as the ``signatories`` of the *Agreement* contract.

    All of the ``toSign`` parties have the choice to ``Sign``. This choice checks that the party is indeed a member of ``toSign``, then creates a new instance of the *Pending* contract where they have been moved to the ``signed`` list.

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: first half pending template
        :end-before: -- end snippet: first half pending template

    Once all of the parties have signed, any of them can create the final Agreement contract using the ``Finalize`` choice. This checks that all of the signatories for the *Agreement* have signed the *Pending* contract.

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: second half pending template
        :end-before: -- end snippet: second half pending template

Collecting the signatures in practice
    Since the final Pending contract has multiple signatories, **it cannot be created in that state by any one stakeholder**.

    However, a party can create a pending contract, with all of the other parties in the ``toSign`` list. 

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: testing setup
        :end-before: -- end snippet: testing setup

    Once the Pending contract is created, the other parties can sign it. For simplicity, the example code only has choices to express consensus (but you might want to add choices to Accept, Reject, or Negotiate).

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: testing add agreements
        :end-before: -- end snippet: testing add agreements

    Once all of the parties have signed the Pending contract, any of them can then exercise the ``Finalize`` choice. This creates the Agreement contract on the ledger.

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: testing finalize
        :end-before: -- end snippet: testing finalize

.. figure:: images/multiplepartyAgreement.png
  :figwidth: 80%
  :alt: The Multiparty Agreement pattern, in which the Pending contract recreates itself each time a party signs until all have signed and one exercises the Finalize choice to create the Agreement contract. 

  Multiple Party Agreement Diagram
