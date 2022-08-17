Tutorials : How to implement a Contingent Claims-based instrument
#################################################################

In this chapter we will look at how to create a strongly typed instrument, which leverages
the ``Contingent Claims`` library.
As an example, we will see how the fixed rate bond instrument is implemented in Daml Finance.
The goal is that you will learn how to implement your own instrument template, if you need an
instrument type that is not already implemented in Daml Fincance.

Template definition
===================

We start by defining a new template for the instrument. Here are the first few lines of the fixed rate instrument:

.. code:: daml

  -- | This template models a fixed rate bond.
  -- It pays a fixed coupon rate at the end of every coupon period.
  template Instrument
    with
      depository : Party
        -- ^ The depository of the instrument.
      issuer : Party
        -- ^ The issuer of the instrument.
      id : Id
        -- ^ An identifier of the instrument.
      couponRate : Decimal
        -- ^ The fixed coupon rate, per annum. For example, in case of a "3.5% p.a coupon" this should be 0.035.

The ``Contingent Claims`` tree is not part the template. Instead, it will be created
dynamically, as described in the next sections.

HasClaims interface
===================

In order for the instrument to work with the general Daml Finance lifecycling framework
we will implement the HasClaims interface. This provides a generic mechanism to
process coupon payments and the redemption amount. The good thing here is that it will
work in a similar way for all instrument types, regardless of their economic terms.

Here is a high level implementation of HasClaims:

.. code:: daml

    implements HasClaims.I where
      view = HasClaims.View with acquisitionTime = dateToDateClockTime issueDate
      getClaims = do
        -- get the initial claims tree (as of the bond's acquisition time)
        schedule <- createCouponSchedule firstCouponDate holidayCalendarIds businessDayConvention couponPeriod couponPeriodMultiplier issueDate maturityDate issuer calendarDataProvider
        couponClaims <- createFixRateCouponClaims schedule couponRate dayCountConvention currency
        redemptionClaim <- createRedemptionClaim currency maturityDate
        pure $ mconcat [couponClaims, redemptionClaim]

First, we create a coupon schedule, which depends on the coupon dates and a holiday calendar.
This is then used to create the actual coupon claims.
The redemption claim is also created.
Finally, the the coupon claims and the redemption claim define the economic terms of the instrument.

How to define the redemption claim
==================================

In the above example, we see that the redemption claim depends on the currency and the maturity date.

We will now create a ``Contingent Claims`` representation of the actual redemption claim:

.. code:: daml

  -- | Create a redemption claim
  createRedemptionClaim : Applicative f => Deliverable -> Date -> f [TaggedClaim]
  createRedemptionClaim cashInstrumentCid maturityDate = do
    let
      redemptionClaim = [when (TimeGte $ maturityDate) $ one cashInstrumentCid]
    prepareAndTagClaims redemptionClaim "Redemption"


How to define the coupon claims
===============================

The coupon claims are a bit more complicated to define.
We need to take a schedule of adjusted coupon dates and the day count convention into account.

Here is how we create the ``Contingent Claims`` representation of the coupons:

.. code:: daml

  -- | Calculate a fix coupon amount for each coupon date and create claims
  createFixRateCouponClaims : (HasField "adjustedEndDate" r Date, HasField "adjustedStartDate" r Date, Applicative f) => [r] -> Decimal -> DayCountConventionEnum -> Deliverable -> f [TaggedClaim]
  createFixRateCouponClaims schedule couponRate dayCountConvention cashInstrumentCid = do
    let
      couponDatesAdjusted = map (.adjustedEndDate) schedule
      couponAmounts = map (\p -> couponRate * (calcDcf dayCountConvention p.adjustedStartDate p.adjustedEndDate)) schedule
      couponClaims = zipWith (\d a -> when (TimeGte $ d) $ scale (Const a) $ one cashInstrumentCid) couponDatesAdjusted couponAmounts
    prepareAndTagClaims couponClaims "Fix Coupon"

For each coupon period we calculate the adjusted end date and the amount of the coupon.
We then create each coupon claim in a way similar to the redemption claim above.

How the instrument evolves over time
====================================

The bond instrument gives the holder the right to receive future coupons and the redemption amount.
At issuance, this means all the coupons, since they are all in the future.
However, when the first coupon is paid, the holder of the instrument is no longer entitled to receive this coupon again.
In other words, the claims representation of the instrument changes. It evolves over time.

In our implementation of the fixed rate bond we want a simple and reliable mechanism for evolving the instrument.
Luckily for us, when the lifecycle function returns a coupon to be paid today, it also returns the remaining claims of the instrument
(excluding today's and any previous coupons). Hence, we can use this to evolve our instrument, in a way that is guaranteed to be
consistent with the lifecycle mechanism.

This is all done in the ``processClockUpdate`` function. We will now break it apart to describe the steps in more detail:

.. code:: daml

  -- | Rule to process a clock update event.
  processClockUpdate : IsBond t => Party -> ContractId Event.I -> ContractId Clock.I -> ContractId Lifecyclable.I -> t -> [ContractId Observable.I] -> Update (ContractId Lifecyclable.I, [ContractId Effect.I])
  processClockUpdate settler eventCid _ self instrument observableCids = do
    t <- Event.getEventTime <$> fetch eventCid
    let
      claimInstrument = toInterface @HasClaims.I instrument
      acquisitionTime = HasClaims.getAcquisitionTime claimInstrument

    -- Recover claims tree as of the lastEventTimestamp. For a bond, this just requires lifecycling as of the lastEventTimestamp
    initialClaims <- HasClaims.getClaims claimInstrument

First, we retrieve the inital claims of the instrument.
This represents the bond as of inception.
By keeping track of ``lastEventTimestamp`` (in our case: the last time a coupon was paid),
we can "fast forward" to the remaining claims of the instrument:

.. code:: daml

    claims <- Prelude.fst <$> lifecycle observableCids claimInstrument [timeEvent instrument.lastEventTimestamp]

Finally, we can lifecycle the instrument as of the current time (as descibed by the Clock template).
If there is a lifecycle effect (for example a coupon), we will create an Effect for it, which can then be settled.

.. code:: daml

    -- Lifecycle
    (remaining, pending) <- lifecycleClaims observableCids acquisitionTime claims [timeEvent t]
    let
      (consumed, produced) = splitPending pending
    if remaining == claims && null pending then
      pure (self, [])
    else do
      let
        currentKey = Instrument.getKey $ toInterface @Instrument.I instrument
        settlementDate = toDateUTC t -- TODO remove this dependency
        newKey = currentKey with id.version = sha256 $ show remaining
      newInstrumentCid <- create instrument with lastEventTimestamp = t; id = newKey.id
      Instrument.createReference instrument.issuer $ toInterfaceContractId newInstrumentCid
      effectCid <- toInterfaceContractId <$> create Effect with
        provider = currentKey.issuer
        settler
        targetInstrument = currentKey
        producedInstrument = if isZero' remaining then None else Some newKey
        consumed
        produced
        settlementDate
        id = instrument.id.label <> "-" <> show settlementDate
        observers = (.observers) . Disclosure.view $ toInterface @Disclosure.I instrument
      pure (toInterfaceContractId newInstrumentCid, [effectCid])


Observables
===========

In our fixed rate bond example above, the coupon amount is pre-determined at the
inception of the instrument. In contrast, a floating rate coupon is defined by the
value of a reference rate during the lifetime of the bond. Since we do not know this
value when the instrument is created, we need to define the coupon based on a
future observation of the reference rate.
In order to do this we introduce the concept of an ``Observable``.

In the instrument definition, we need an identifier for the reference rate:

.. code:: daml

  template Instrument
    with
      referenceRateId : Text
        -- ^ The floating rate reference ID. For example, in case of "3M Euribor + 0.5%" this should a valid reference to the "3M Euribor" reference rate.

In the claims definition, we can then use ``Observe`` to refer to the value of the reference rate:

.. code:: daml

  -- | Calculate a floating coupon amount for each coupon date and create claims
  createFloatingRateCouponClaims : (HasField "adjustedEndDate" r Date, HasField "adjustedStartDate" r Date, Applicative f) => [r] -> Decimal -> DayCountConventionEnum -> Deliverable -> Observable -> f [TaggedClaim]
  createFloatingRateCouponClaims schedule couponSpread dayCountConvention cashInstrumentCid referenceRateId = do
    let
      couponClaims = map (\p ->
        when (TimeGte $ p.adjustedStartDate) $ scale ((Observe referenceRateId + Const couponSpread) * (Const (calcDcf dayCountConvention p.adjustedStartDate p.adjustedEndDate))) $
        when (TimeGte $ p.adjustedEndDate) $ one cashInstrumentCid) schedule
    prepareAndTagClaims couponClaims "Floating Coupon"

In this example, the reference rate is the observable. Other instrument types can require other
types of observables, for example an FX rate or a stock price.
