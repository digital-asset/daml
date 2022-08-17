Tutorials : How to use the Bond extension package
#################################################

How to create a bond instrument
*******************************

There are different types of bonds, which mainly differ in the way the coupon is defined.
In order to create a bond instrument you first have to decide what type it is.
The bond extension package currently supports the following bond types:

Fixed rate
==========

Fixed rate bonds pay a constant coupon each coupon period. The coupon is quoted on a yearly basis (per annum, p.a.), but it could be paid more frequently.
For example, a bond could have a 2% p.a. coupon and a 6M coupon period. That would mean
a 1% coupon is paid twice a year.

Here is an example of a bond paying a 1.1% p.a. coupon with a 12M coupon period:

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



Floating rate
=============

Floating rate bonds pay a coupon which is determined by a reference rate.
There is also a rate spread, which is paid in addition to the reference rate.

Here is an example of a bond paying Euribor 3M + 1.1% p.a. with a 3M coupon period:

.. code:: daml

  let
    issueDate = date 2019 Jan 16
    firstCouponDate = date 2019 May 15
    maturityDate = date 2020 May 15
    referenceRateId = "EUR/EURIBOR/3M"
    couponRate = 0.011
    couponPeriod = M
    couponPeriodMultiplier = 3
    redemptionAmount = 1_000_000.0
  bondInstrument <- originateFloatingRateBond custodian issuer "BOND" obs now issueDate holidayCalendarId calendarDataProvider firstCouponDate maturityDate dayCountConvention businessDayConvention couponRate couponPeriod couponPeriodMultiplier cashInstrumentCid referenceRateId
  investorBondTransferableCid <- Account.credit [publicParty] bondInstrument redemptionAmount investorAccount

The reference rate is observed once at the beginning of each coupon period and used for
the coupon payment at the end of that period.

Inflation linked
================

Inflation linked bonds pay a fixed coupon rate at the end of every coupon period.
The coupon is calculated based on a principal that is adjusted according to an inflation index,
for example the Consumer Price Index (CPI) in the U.S.

Here is an example of a bond paying 1.1% p.a. (on a CPI adjusted principal) with a 3M coupon period:

.. code:: daml

  let
    issueDate = date 2019 Jan 16
    firstCouponDate = date 2019 May 15
    maturityDate = date 2020 May 15
    inflationIndexId = "CPI"
    couponRate = 0.011
    couponPeriod = M
    couponPeriodMultiplier = 3
    redemptionAmount = 1_000_000.0
  bondInstrument <- originateInflationLinkedBond custodian issuer "BOND" obs now issueDate holidayCalendarId calendarDataProvider firstCouponDate maturityDate dayCountConvention businessDayConvention couponRate couponPeriod couponPeriodMultiplier cashInstrumentCid inflationIndexId inflationIndexBaseValue
  investorBondTransferableCid <- Account.credit [publicParty] bondInstrument redemptionAmount investorAccount

At maturity, the greater of the adjusted principal and the original principal is redeemed.
For clarity, this only applies to the redemption amount. The coupons are always calculated based on the adjusted principal.
This means that in the case of deflation, the coupons would be lower than the specified coupon rate but the original principal would still be redeemed at maturity.


Zero coupon
===========

A zero coupon bond does not pay any coupons at all.
It only pays the redemption amount at maturity.

Here is an example of a zero coupon bond:

.. code:: daml

  let
    issueDate = date 2019 Jan 16
    maturityDate = date 2020 May 15
    redemptionAmount = 1_000_000.0

  bondInstrument <- originateZeroCouponBond custodian issuer "BOND" obs now issueDate maturityDate cashInstrumentCid
  investorBondTransferableCid <- Account.credit [publicParty] bondInstrument redemptionAmount investorAccount


How to trade and transfer a bond
********************************

When you have created a holding on a bond instrument this can be transfered to another party.
This is described in :doc:`Getting Started: Transfer <../getting-started/getting-started>`.

In order to trade a bond (transfer it in exchange for cash) you can also initiate a delivery versus payment with atomic settlement.
This is described in :doc:`Getting Started: Settlement <../getting-started/settlement>`.

How to process coupon payments
******************************

On the coupon payment date, the issuer will need to lifecycle the bond.
This will result in a lifecycle effect for the coupon, which can be cash settled.
This is described in detail in :doc:`Getting Started: Lifecycling <../getting-started/lifecycling>`.

How to redeem a bond
********************

On the redemption date, both the last coupon and the redemption amount with be paid.
This is processed in the same way as a single coupon payment described above.
