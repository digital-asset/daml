.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Lock by Safekeeping
###################

Safekeeping is a realistic way to model locking as it is a common practice in many industries. For example, during a real estate transaction, purchase funds are transferred to the sellers lawyer’s escrow account after the contract is signed and before closing. To understand its implementation, review the original *Coin* template first.

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

There is no need to make a change to the original contract. With two additional contracts, we can transfer the *Coin* ownership to a locker party.

* Introduce a separate contract template *LockRequest* with the following features:

  - LockRequest has a locker party as the single signatory, allowing the locker party to unilaterally initiate the process and specify locking terms.
  - Once owner exercises *Accept* on the lock request, the ownership of coin is transferred to the locker.
  - The *Accept* choice also creates a *LockedCoinV2* that represents *Coin* in locked state.

.. literalinclude:: ../daml/LockingBySafekeeping.daml
  :language: daml
  :start-after: -- BEGIN_LOCK_REQUEST_TEMPLATE
  :end-before: -- END_LOCK_REQUEST_TEMPLATE

* *LockedCoinV2* represents *Coin* in the locked state. It is fairly similar to the *LockedCoin* described in :ref:`lockingbyConsumingChoice`.  The additional logic is to transfer ownership from the locker back to the owner when *Unlock* or *Clawback* is called.

.. literalinclude:: ../daml/LockingBySafekeeping.daml
  :language: daml
  :start-after: -- BEGIN_LOCKED_COIN_V2_TEMPLATE
  :end-before: -- END_LOCKED_COIN_V2_TEMPLATE

.. figure:: ../images/lockingBySafekeeping.png
   :alt: Locking by Safekeeping diagram, showing the ownership transfer of Coin and the creation of LockedCoin

   Locking By Safekeeping Diagram

Trade-offs
**********

Ownership transfer may give the locking party too much access on the locked asset. A rogue lawyer could run away with the funds. In a similar fashion, a malicious locker party could introduce code to transfer assets away while they are under their ownership.

