Getting started 3 : Lifecycling
###############################

This tutorial describes the :ref:`lifecycle <lifecycling>` flow between two counterparties.
We will use a bond instrument to illustrate the different steps:

#. Creating a fixed-rate bond instrument
#. Defining the clock for time-based events
#. Lifecycling the bond instrument
#. Settling the instructions

Creating a fixed-rate bond instrument
=====================================

We start by defining a fixed rate bond, which pays a 1.1% coupon every year. We also credit the account of an investor:

.. code:: daml

  let
    issueDate = date 2019 Jan 16
    firstCouponDate = date 2019 May 15
    maturityDate = date 2020 May 15
    couponRate = 0.011
    couponPeriod = M
    couponPeriodMultiplier = 12
    redemptionAmount = 1_000_000.0
  bondInstrument <- originateFixedRateBond custodian issuer "BOND" obs now issueDate holidayCalendarIds calendarDataProvider firstCouponDate maturityDate dayCountConvention businessDayConvention couponRate couponPeriod couponPeriodMultiplier cashInstrumentCid
  investorBondTransferableCid <- Account.credit [publicParty] bondInstrument redemptionAmount investorAccount



Defining the clock for time-based events
========================================

Since the bond pays a coupon on a yearly basis, we talk about a time-based event.
The requirement to pay the coupon is governed by actual time.
However, in a trading and settlement system, it is useful to be able to control
the time variable, in order to simulate previous/future payments, or to have some flexibility
regarding when to process events.

We define a clock contract to control the passage of time:

.. code:: daml

  -- create clock and clock update event
  let clock = DateClock with u = Unit today; id = show today; provider = issuer; observers = empty
  clockCid <- toInterfaceContractId <$> submitMulti [issuer] [] do createCmd clock
  clockEventCid <- toInterfaceContractId <$> submitMulti [issuer] [] do createCmd DateClockUpdateEvent with id = "Update to " <> show today, clock


Lifecycling the bond instrument
===============================

We use the ``Lifecyclable`` interface, which is defined in ``Daml.Finance.Interface.Lifecycle.Lifecyclable``.

The issuer of the bond is responsible for initiating the coupon payment,
by calling ``Lifecycle`` on the coupon date:

.. code:: daml

  -- Try to lifecycle bond
  (bondLifecyclableCid2, effectCids) <- Instrument.submitExerciseInterfaceByKeyCmd @Lifecyclable.I [issuer] readAs bondInstrument
    Lifecyclable.Lifecycle with settler; eventCid = clockEventCid; observableCids; ruleName = "Time"; clockCid

This internally uses the ``Event`` interface, which is defined in ``Daml.Finance.Interface.Lifecycle.Event``. In our case, the event
is a clock event, since the coupon is defined by the passage of time.

The ``effectCids`` will contain the effect(s) of the lifecycling, in this case a coupon payment.
If there is nothing to lifecycle, for example because there is no coupon to be paid today, ``effectCids`` would be empty.
The ``Effect`` interface is defined in ``Daml.Finance.Interface.Lifecycle.Effect``.



Settling the instructions
=========================

In order to process the effect(s) of the lifecycling (in this case: pay the coupon), we need to create settlement instructions.
We start by creating a settlement factory:

.. code:: daml

  -- Create settlement factory
  factoryCid <- submitMulti [investor] [] do createCmd BatchFactory with requestors = singleton investor

The investor then claims the effect:

.. code:: daml

  -- Claim effect
  settlementRuleCid <- submitMulti [custodian, investor] [] do
    createCmd Rule
      with
        custodian
        owner = investor
        claimers = singleton investor
        settler
        instrumentLabel = bondInstrument.id.label
        instructableCid = toInterfaceContractId factoryCid

  result <- submitMulti [investor] readAs do
    exerciseCmd settlementRuleCid SettlementRule.Claim with
      claimer = investor
      holdingCids = [toInterfaceContractId @Holding.I investorBondTransferableCid]
      effectCid

Finally, the settlement instructions are allocated, approved and then settled.

.. code:: daml

  let
    Some [investorBondHoldingCid] = result.newInstrumentHoldingCids
    [custodianCashInstructionCid] = result.instructionCids

  -- Allocate instructions
  custodianCashInstructionCid <- submitMulti [custodian] readAs do exerciseCmd custodianCashInstructionCid Instruction.Allocate with transferableCid = custodianCashTransferableCid

  -- Approve instructions
  custodianCashInstructionCid <- submitMulti [investor] [] do
    exerciseCmd custodianCashInstructionCid Instruction.Approve with receiverAccount = investorAccount

  -- Settle container
  [investorCashTransferableCid] <- submitMulti [settler] [] do exerciseCmd result.containerCid Settleable.Settle

This is the result of the settlement:
  - The investor receives cash for the coupon.
  - The investor receives a new version of the bond instrument, which excludes today's coupon (it only contains future coupons and the redemption amount).
  - The issuer receives the original version of the bond instrument, which can be archived.
