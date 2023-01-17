.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _explicit-contract-disclosure:

(Experimental) Explicit contract disclosure
###########################################

Starting with Canton 2.6, explicit contract disclosure is introduced as an (experimental) feature
that allows delegating contract read rights to non-stakeholder parties in an optimal manner.
Until now, read delegation was possible as well through the following mechanisms:

- As described in :ref:`Divulgence: When Non-Stakeholders See Contracts <da-model-divulgence>`,
  one way of delegating read rights of a contract **C** to a non-stakeholder party **P** is by divulging **C** to **P**.
  However, usage of divulged contracts in submitted commands is deprecated and not compatible with ledger pruning.
  Furthermore, this mechanism requires the contract's stakeholder to explicitly divulge the contract to all necessary readers,
  limiting scalability.
- Another alternative for achieving read delegation is to make use of the multi-party submission feature,
  by including the contract's stakeholder in the command submission's ``readAs``.
  However, this option has downsides as well:

    - The granularity is too coarse (e.g. by using a reader party, one discloses everything visible to that party rather than individual contracts).
    - The reader party needs to be co-hosted with the actor party on the same participant, limiting the usage in distributed environments.

Read delegation using explicit contract disclosure
--------------------------------------------------

Explicit contract disclosure simplifies read delegation and solves the scalability and visibility granularity caveats
mentioned above in the pre-existing mechanisms.
It does so by allowing the stakeholder **S** of a contract **C** to `explicitly disclose` its contract to other non-stakeholder parties.
In turn, a party **P** which is neither a stakeholder nor an informee of **C** but has received **C**
from **S**, can act as a submitter of a command which needs to use **C**
by attaching the payload of **C** as a disclosed contract to the command submission.

.. note:: The Ledger API uses the disclosed contracts attached to command submissions
  for resolving contract and key activeness lookups during command interpretation.
  This means that usage of a disclosed contract effectively bypasses the visibility restriction
  of the submitting party's over the respective contract.
  However, the authorization restrictions of the Daml model still apply:
  the submitted command still needs to be well authorized (i.e. the actors
  need to be properly authorized to execute the action -
  as described in :ref:`Privacy Through Authorization <da-model-privacy-authorization>`).

If we return to the example depicted in :ref:`Divulgence: When Non-Stakeholders See Contracts <da-model-divulgence>`,
**ShowIou** is an auxiliary contract used by Alice to fetch the `IOU` under the Painter's
projection in order to divulge it to him so that the Painter can use it when accepting the `CounterOffer`.
This auxiliary step can be omitted if instead Alice chooses to :ref:`share <submitter-disclosed-contracts-share>` the `IOU`
with the Painter so the Painter can use it as a disclosed contract in the command submission where he is accepting the `CounterOffer`.

.. note:: Usage of disclosed contracts in command submission can be enabled by toggling
  the ``explicit-disclosure-unsafe`` flag in the participant configuration as exemplified below.
  However, the feature is **experimental** and **must** not be used in production environments.

::

    participants {
        participant {
            ledger-api.explicit-disclosure-unsafe = true
        }
    }

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
