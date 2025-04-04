.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

The Propose and Accept Pattern
###############################

The Propose and Accept pattern demonstrates how to start a bilateral workflow. One party creates a proposal or an invite contract. This gives another party the chance to accept, reject, or renegotiate.

Motivation
**********

It takes two to tango, but one party has to propose. It is no different in the business world. The contractual relationship between two businesses often starts with an invite, a business proposal, a bid offering, etc.

Invite
  When a market operator wants to set up a market, they need to go through an onboarding process in which they invite participants to sign master service agreements and fulfill different roles in the market. Receiving participants need to evaluate the rights and responsibilities of each role and respond accordingly.
Propose
  When issuing an asset, an issuer is making a business proposal to potential buyers. The proposal lays out what is expected from buyers, and what they can expect from the issuer. Buyers need to evaluate all aspects of the offering, e.g. price, return, and tax implications, before making a decision.

The Propose and Accept pattern demonstrates how to write a Daml program to model the initiation of an inter-company contractual relationship. Daml modelers often have to follow this pattern to ensure that no participant is forced into an obligation.

Implementation
**************

The Propose and Accept pattern in general involves two contracts, the proposal contract and the result contract:

Proposal Contract
  The proposal contract can be created from a role contract or any other point in the workflow. In this example, the proposal contract is the proposal contract *CoinIssueProposal* which the issuer created from the master contract *CoinMaster*.

  .. literalinclude:: daml/CoinIssuance.daml
    :language: daml
    :start-after: -- BEGIN_COIN_ISSUER
    :end-before: -- END_COIN_ISSUER

  The *CoinIssueProposal* contract has *Issuer* as the signatory and *Owner* as the controller to the *Accept* choice. In its complete form, the *CoinIssueProposal* contract should define all choices available to the owner, i.e. Accept, Reject or Counter (re-negotiate terms).

  .. literalinclude:: daml/CoinIssuance.daml
    :language: daml
    :start-after: -- BEGIN_COIN_ISSUE_PROPOSAL
    :end-before: -- END_COIN_ISSUE_PROPOSAL

Result Contract
  Once the owner exercises the *AcceptCoinProposal* choice on the proposal contract to express their consent, it returns a result contract representing the agreement between the two parties. In this example, the result contract is of type *CoinIssueAgreement*. Note, it has both *issuer* and *owner* as the signatories, implying they both need to consent to the creation of this contract. Both parties could be controller(s) on the result contract, depending on the business case.

  .. literalinclude:: daml/CoinIssuance.daml
    :language: daml
    :start-after: -- BEGIN_COIN_ISSUE_AGREEMENT
    :end-before: -- END_COIN_ISSUE_AGREEMENT

.. figure:: images/initiateaccept.png
   :alt: The Propose and Accept Pattern, showing how the CoinIssueProposal contract (a proposal contract), when accepted, returns the CoinIssueAgreement result contract.

   Propose and Accept pattern diagram

Trade-offs
**********

Propose and Accept can be quite verbose if signatures from more than two parties are required to progress the workflow.
