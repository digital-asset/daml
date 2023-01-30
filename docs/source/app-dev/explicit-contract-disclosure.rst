.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _explicit-contract-disclosure:

Explicit contract disclosure (experimental)
###########################################

Starting with Canton 2.6, explicit contract disclosure is introduced as an (experimental) feature
that allows seamlessly delegating contract read rights to a non-stakeholder using off-ledger data distribution.

.. note:: Usage of disclosed contracts in command submission by means of explicit contract disclosure
  can be enabled by toggling the ``explicit-disclosure-unsafe`` flag in the participant configuration as exemplified below.
  However, the feature is **experimental** and **must** not be used in production environments.

::

    participants {
        participant {
            ledger-api.explicit-disclosure-unsafe = true
        }
    }

Contract read delegation
------------------------

Contract read delegation allows a party to acquire read rights during
command submission over a contract of which it is neither a stakeholder nor an informee.

As an example application where read delegation could be used,
let's consider a simplified swap transaction between two parties.
In this example, each party has a football NFT card, modeled below as ``FootballCardNft``,
that they want to swap.

::

    template ``FootballCardNft``
      with
        owner: Party
        issuer: Party
        playerReference: Text
      where
        signatory [owner, issuer]
        key (issuer, playerReference): (Party, Text)
        maintainer key._1

        choice Transfer: ContractId FootballCardNft
          with newOwner: Party
          controller [owner, newOwner]
            do create this with owner=newOwner

Each ``FootballCardNft`` has as signatories the ``owner`` and the ``issuer`` and a ``playerReference``
referencing a football player. The ``playerReference`` is used to uniquely identify a NFT
issued by a commonly-trusted authority ``issuer``. The uniqueness constraint is
ensured by the ``(issuer, playerReference)`` template key.
The ``Transfer`` choice can be used to transfer the card to another party.

Going forward with our example, let's consider party **Alice** that wants to swap her card
with ``playerReference`` **P1** with a card for **P2**, both issued
by a common NFT card ``issuer``.

Now, for the swap to happen, one of the parties involved can put forward an ``Offer``, as modeled below.

::

    template Offer
      with
        cardToSwapCid: ContractId FootballCardNft
        expectedPlayerInSwap: Text
        seller: Party
      where
        signatory seller

        choice Accept: ContractId FootballCardNft
          with
            buyer: Party
            otherCardCid: ContractId FootballCardNft
          controller buyer
          do
            otherCard <- fetch otherCardCid
            otherCard.playerReference === expectedPlayerInSwap
            exercise otherCardCid Transfer with newOwner=seller
            exercise cardToSwapCid Transfer with newOwner=buyer

The ``Offer`` template contains the contract id of the offered card ``cardToSwapCid``,
the card player reference that is expected in return (``expectedPlayerInSwap``) and the ``seller``.

Now, let's assume that **Alice** wants to swap her NFT with any party holding the card for football player **P2**,
without a preference over the exchanging counterparty.

Hence, **Alice** creates an ``Offer`` with her as the ``seller`` and the ``expectedPlayerInSwap`` as the player
that she wants in return (**P2**) for her card. Once the offer is created, **Bob** can exercise ``Accept`` on **Alice**'s offer
and perform the swap atomically.

But since **Bob** is not a stakeholder or prior informee on **Alice**'s offer,
how can he exercise ``Accept`` on the it? Furthermore, how can **Bob** fetch
**Alice**'s card in the ``Accept`` choice without having seen it either?

Read delegation using explicit contract disclosure
``````````````````````````````````````````````````

With the introduction of explicit contract disclosure, **Bob** can accept the offer from **Alice**
without having seeing it or her ``FootballCardNft`` before on the ledger. This is possible if **Alice** decides to disclose
(or :ref:`share <submitter-disclosed-contracts-share>`) her contracts' details to any party desiring to do the swap.
**Bob** can attach the details of these contracts as disclosed contracts in the command submission
that he is using to exercise ``Accept`` on **Alice**'s offer, thus bypassing the visibility restriction
that he had over **Alice**'s contracts.

.. note:: The Ledger API uses the disclosed contracts attached to command submissions
  for resolving contract and key activeness lookups during command interpretation.
  This means that usage of a disclosed contract effectively bypasses the visibility restriction
  of the submitting party's over the respective contract.
  However, the authorization restrictions of the Daml model still apply:
  the submitted command still needs to be well authorized (i.e. the actors
  need to be properly authorized to execute the action -
  as described in :ref:`Privacy Through Authorization <da-model-privacy-authorization>`).

.. _submitter-disclosed-contracts-share:

How does the stakeholder disclose its contract to submitters?
-------------------------------------------------------------

The disclosed contract's details can be fetched by the contract's stakeholder from the contract's
associated :ref:`CreatedEvent <com.daml.ledger.api.v1.CreatedEvent>`,
which can be read from the Ledger API via the active contracts and transactions queries
(see :ref:`Reading from the ledger <reading-from-the-ledger>`).

The stakeholder can then share the disclosed contract details to the submitter off-ledger (i.e. outside of Daml)
by conventional means (e.g. SFTP, e-mail etc.). A :ref:`DisclosedContract <com.daml.ledger.api.v1.DisclosedContract>` can
be constructed from the fields of the same name from the original contract's ``CreatedEvent``.

.. note:: Only contracts created starting with Canton 2.6 can be shared as disclosed contracts.
  Prior to this version, contracts' **CreatedEvent** does not have ``ContractMetadata`` populated
  and cannot be used as disclosed contracts.

Attaching a disclosed contract to a command submission
------------------------------------------------------

A disclosed contract can be attached as part of the ``Command``'s :ref:`disclosed_contracts <com.daml.ledger.api.v1.Commands.disclosed_contracts>`
and requires the following fields (see :ref:`DisclosedContract <com.daml.ledger.api.v1.DisclosedContract>` for content details) to be populated from
the original `CreatedEvent` (see above):

- **template_id** - The contract's template id.
- **contract_id** - The contract id.
- **arguments** - The contract's create arguments. This field is a protobuf ``oneof``
  and it allows either passing the contract's create arguments typed (as ``create_arguments``)
  or as a byte array (as ``create_arguments_blob``).
  Generally, clients should use the ``create_arguments_blob`` for convenience since they can be received as such
  from the stakeholder off-ledger (see above).
- **metadata** - The contract metadata. This field can be populated as received from the stakeholder (see below).
