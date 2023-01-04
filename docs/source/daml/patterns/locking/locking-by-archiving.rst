.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Lock by Archiving
#################

**Pre-condition**: there exists a contract that needs to be locked and unlocked. In this section, *Coin* is used as the original contract to demonstrate locking and unlocking.

.. literalinclude:: ../daml/CoinIssuance.daml
  :language: daml
  :start-after: -- BEGIN_COIN_TEMPLATE_DATATYPE
  :end-before: -- END_COIN_TEMPLATE_DATATYPE

.. literalinclude:: ../daml/CoinIssuance.daml
  :language: daml
  :start-after: -- BEGIN_COIN_TEMPLATE_TRANSFER
  :end-before: -- END_COIN_TEMPLATE_TRANSFER

.. literalinclude:: ../daml/CoinIssuance.daml
  :language: daml
  :start-after: -- BEGIN_COIN_TEMPLATE_ARCHIVE
  :end-before: -- END_COIN_TEMPLATE_ARCHIVE

Archiving is a straightforward choice for locking because once a contract is archived, all choices on the contract become unavailable. Archiving can be done either through consuming choice or archiving contract.

.. _lockingbyConsumingChoice:

Consuming Choice
****************

The steps below show how to use a consuming choice in the original contract to achieve locking:

* Add a consuming choice, *Lock*, to the *Coin* template that creates a *LockedCoin*.
* The controller party on the *Lock* may vary depending on business context. In this example, *owner* is a good choice.
* The parameters to this choice are also subject to business use case. Normally, it should have at least locking terms (eg. lock expiry time) and a party authorized to unlock.

.. literalinclude:: ../daml/CoinIssuance.daml
  :language: daml
  :start-after: -- BEGIN_COIN_TEMPLATE_LOCK
  :end-before: -- END_COIN_TEMPLATE_LOCK

* Create a *LockedCoin* to represent *Coin* in the locked state. *LockedCoin* has the following characteristics, all in order to be able to recreate the original *Coin*:

  - The signatories are the same as the original contract.
  - It has all data of *Coin*, either through having a *Coin* as a field, or by replicating all data of *Coin*.
  - It has an *Unlock* choice to lift the lock.


  .. literalinclude:: ../daml/CoinIssuance.daml
    :language: daml
    :start-after: -- BEGIN_LOCKED_COIN_TEMPLATE_DATATYPE
    :end-before: -- END_LOCKED_COIN_TEMPLATE_DATATYPE

  .. literalinclude:: ../daml/CoinIssuance.daml
    :language: daml
    :start-after: -- BEGIN_LOCKED_COIN_TEMPLATE_UNLOCK
    :end-before: -- END_LOCKED_COIN_TEMPLATE_UNLOCK


.. figure:: ../images/lockingByArchiving1.png
   :alt: Locking By Consuming Choice involves exercising a Lock choice to create a LockedCoin; the LockedCoin differs from the Coin in having an Unlock choice.

   Locking By Consuming Choice Diagram


Archiving Contract
******************

In the event that changing the original contract is not desirable and assuming the original contract already has an *Archive* choice, you can introduce another contract, *CoinCommitment*, to archive *Coin* and create *LockedCoin*.

* Examine the controller party and archiving logic in the *Archives* choice on the *Coin* contract. A coin can only be archived by the issuer under the condition that the issuer is the owner of the coin. This ensures the issuer cannot archive any coin at will.

.. literalinclude:: ../daml/CoinIssuance.daml
  :language: daml
  :start-after: -- BEGIN_COIN_TEMPLATE_ARCHIVE
  :end-before: -- END_COIN_TEMPLATE_ARCHIVE

* Since we need to call the *Archives* choice from *CoinCommitment*, its signatory has to be *Issuer*.

.. literalinclude:: ../daml/CoinCommitment.daml
  :language: daml
  :start-after: -- BEGIN_COIN_COMMITMENT_TEMPLATE_DATATYPE
  :end-before: -- END_COIN_COMMITMENT_TEMPLATE_DATATYPE

* The controller party and parameters on the *Lock* choice are the same as described in locking by consuming choice. The additional logic required is to transfer the asset to the issuer, and then explicitly call the *Archive* choice on the *Coin* contract.
* Once a *Coin* is archived, the *Lock* choice creates a *LockedCoin* that represents *Coin* in locked state.

.. literalinclude:: ../daml/CoinCommitment.daml
  :language: daml
  :start-after: -- BEGIN_COIN_COMMITMENT_TEMPLATE_LOCK_COIN
  :end-before: -- END_COIN_COMMITMENT_TEMPLATE_LOCK_COIN

.. figure:: ../images/lockingByArchiving2.png
   :alt: Locking by Archiving Contract uses the CoinCommitment contract to archive Coin and create LockedCoin

   Locking By Archiving Contract Diagram

Trade-offs
**********

This pattern achieves locking in a fairly straightforward way. However, there are some tradeoffs.

* Locking by archiving disables all choices on the original contract. Usually for consuming choices this is exactly what is required. But if a party needs to selectively lock only some choices, remaining active choices need to be replicated on the *LockedCoin* contract, which can lead to code duplication.
* The choices on the original contract need to be altered for the lock choice to be added. If this contract is shared across multiple participants, it will require agreement from all involved.

.. * The locking party arbitrarily determines the locking terms (e.g. lock expiry date). If the unlocking party needs to negotiate, it will require Propose and Accept contracts between the two parties and complicate the workflows.
