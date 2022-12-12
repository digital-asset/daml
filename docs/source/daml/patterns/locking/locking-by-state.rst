.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Lock by State
#############

The original *Coin* template is shown below. This is the basis on which to implement locking by state

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

In its original form, all choices are actionable as long as the contract is active. Locking by State requires introducing fields to track state. This allows for the creation of an active contract in two possible states: locked or unlocked. A Daml modeler can selectively make certain choices actionable only if the contract is in unlocked state. This effectively makes the asset lockable.

The state can be stored in many ways. This example demonstrates how to create a *LockableCoin* through a party. Alternatively, you can add a lock contract to the asset contract, use a boolean flag or include lock activation and expiry terms as part of the template parameters.

Here are the changes we made to the original *Coin* contract to make it lockable.

* Add a *locker* party to the template parameters.
* Define the states.

  - if owner == locker, the coin is unlocked
  - if owner != locker, the coin is in a locked state

* The contract state is checked on choices.

  - *Transfer* choice is only actionable if the coin is unlocked
  - *Lock* choice is only actionable if the coin is unlocked and a 3rd party locker is supplied
  - *Unlock* is available to the locker party only if the coin is locked

.. literalinclude:: ../daml/LockingByChangingState.daml
  :language: daml
  :start-after: -- BEGIN_LOCKABLE_COIN_TEMPLATE
  :end-before: -- END_LOCKABLE_COIN_TEMPLATE

Locking By State Diagram

.. figure:: ../images/lockingByStateChange.png
   :alt: Locking by State diagram showing that the transfer choice is actionable only if the LockableCoin is unlocked.


Trade-offs
**********

* It requires changes made to the original contract template. Furthermore you should need to change all choices intended to be locked.
* If locking and unlocking terms (e.g. lock triggering event, expiry time, etc) need to be added to the template parameters to track the state change, the template can get overloaded.
