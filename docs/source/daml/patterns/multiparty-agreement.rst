.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Multiple party agreement
########################

The Multiple Party Agreement pattern uses a Pending contract as a wrapper for the Agreement contract. Any one of the signatory parties can kick off the workflow by creating a Pending contract on the ledger, filling in themselves in all the signatory fields. The Agreement contract is not created on the ledger until all parties have agreed to the Pending contract, and replaced the initiator's signature with their own.

Motivation
**********

The :doc:`initaccept` shows how to create bilateral agreements in DAML. However, a project or a workflow often requires more than two parties to reach a consensus and put their signatures on a multi-party contract. For example, in a large construction project, there are at least three major stakeholders: Owner, Architect and Builder. All three parties need to establish agreement on key responsibilities and project success criteria before starting the construction.

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
    The *Pending* contract needs to contain the contents of the proposed *Agreement* contract so that parties know what they are agreeing to, and when all parties have signed, the *Agreement* contract can be created.

    The *Pending* contract has the same list of signatories as the *Agreement* contract. Each party of the *Agreement* has a choice(s) to add their signature.

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: first half pending template
        :end-before: -- end snippet: first half pending template

    One of the stakeholders acts as the coordinator, and has a choice to create the final Agreement contract once all parties have signed.

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: second half pending template
        :end-before: -- end snippet: second half pending template

Collecting the signatures in practice
    Since the final Pending contract has multiple signatories, it cannot be created in that state by any one stakeholder. However, a party can create a pending contract with itself in all signatory slots.

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: testing setup
        :end-before: -- end snippet: testing setup

    Once the Pending contract is created, the other parties can exercise choices to Accept, Reject, or Negotiate. For simplicity, the example code only has choices to express consensus.

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: testing add agreements
        :end-before: -- end snippet: testing add agreements

    The coordinating party can create the Agreement contract on the ledger, and finalize the process when all parties have signed and agreed to the multi-party agreement.

    .. literalinclude:: daml/MultiplePartyAgreement.daml
        :language: daml
        :start-after: -- start snippet: testing finalize
        :end-before: -- end snippet: testing finalize

.. figure:: images/multiplepartyAgreement.png
  :figwidth: 80%

  Multiple Party Agreement Diagram
