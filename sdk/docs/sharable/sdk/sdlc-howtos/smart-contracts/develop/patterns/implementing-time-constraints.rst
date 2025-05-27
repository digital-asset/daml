.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _implementing-time-constraints:

How To Implement Time Constraints
#################################

How to check that a deadline is valid
*************************************

This design pattern demonstrates how to limit choices so that they must occur by a given deadline.

Here is an implementation of an authorized coin transfer:

.. literalinclude:: daml/CoinTransferWithAuthorization.daml
    :language: daml
    :start-after: -- BEGIN_COIN_OWNER_AUTH_TEMPLATE
    :end-before: -- END_COIN_OWNER_AUTH_TEMPLATE

.. literalinclude:: daml/CoinTransferWithAuthorization.daml
    :language: daml
    :start-after: -- BEGIN_ACCEPT_COIN_TRANSFER
    :end-before: -- END_ACCEPT_COIN_TRANSFER

Whilst transfer proposals need to be authorized, they can occur at any point in time. The following changes fix this deficiency:

TransferProposal contract
    In the TransferProposal contract, the body of the AcceptTransfer choice is modified to assert that the contract deadline is valid.

    .. literalinclude:: ./daml/LimitedTimeCoinTransfer.daml
      :language: daml
      :start-after: -- BEGIN_LIMITED_TIME_ACCEPT_COIN_TRANSFER
      :end-before: -- END_LIMITED_TIME_ACCEPT_COIN_TRANSFER

Coin contract
    In the Coin contract, the Transfer choice has an additional deadline argument, so that TransferProposal contracts can
    be given a fixed lifetime.

    .. literalinclude:: ./daml/LimitedTimeCoinTransfer.daml
      :language: daml
      :start-after: -- BEGIN_LIMITED_TIME_COIN_TRANSFER
      :end-before: -- END_LIMITED_TIME_COIN_TRANSFER

.. mermaid::

  stateDiagram-v2
    asOwner: actAs Me
    asNewOwner: actAs You

    state XX <<choice>>

    [*] --> coinCid

    state asOwner {
      coinCid --> X: exercise Transfer(newOwner=You, deadline=now + 5 minutes)
      state TransferBody {
        X --> transferProposalCid: create TransferProposal(coin=coinCid, newOwner=You, deadline=coin.deadline)
        transferProposalCid --> transferProposal
      }
    }
    state asNewOwner {
      transferProposal --> XX: exercise AcceptTransfer()
      state AcceptTransferBody {
        XX --> XXX: now <= coin.deadline
        XXX --> newCoinCid: create Coin(owner=You, issuer=Bank, amount=100)
        XX --> Abort: now > coin.deadline
      }
    }

    newCoinCid --> [*]


How to check that a deadline has passed
***************************************

TODO: https://github.com/DACH-NY/docs-website/issues/315

Grant time-limited writes to parties
************************************

TODO: https://github.com/DACH-NY/docs-website/issues/317

Where to use getTime
********************

TODO: https://github.com/DACH-NY/docs-website/issues/328
