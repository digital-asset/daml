Tutorials : How to use the Derivative extension to model generic instruments
############################################################################

How to create a generic instrument
**********************************

The ``Derivative`` extension provides a flexible framework to model generic instruments in Daml Finance.
It encapsulates the ``Contingent Claims`` library, which allows us to model the economic terms of an instrument.

Defining the claim
==================

Consider a fixed rate bond which pays a 4% p.a. coupon with a 6M coupon period.
Assume there are two coupons remaining until maturity: one today and one in 180 days.
This could be modeled in the following way using ``Contingent Claims``:

.. code:: daml

  -- Create and distribute bond
  let
    today = toDateUTC now
    expiry = addDays today 180
    claims = mapClaimToUTCTime $ mconcat
      [ when (TimeGte $ today) $ scale (Const 0.02) $ one cashInstrument
      , when (TimeGte $ expiry) $ scale (Const 0.02) $ one cashInstrument
      , when (TimeGte $ expiry) $ scale (Const 1.0) $ one cashInstrument
      ]

Now that we have specified the economic terms we can create a derivative instrument:

.. code:: daml

  derivativeInstrument <- originateDerivative csd issuer "BOND" now claims obs now

This will create an instrument containing the ``Contingent Claims`` tree on the ledger.

How to trade and transfer a generic instrument
**********************************************

When you have created a holding on the instrument above it can be transfered to another party.
This is described in :doc:`Getting Started: Transfer <../getting-started/getting-started>`.

In order to trade the instrument (transfer it in exchange for cash) you can also initiate a delivery versus payment with atomic settlement.
This is described in :doc:`Getting Started: Settlement <../getting-started/settlement>`.

How to process lifecycle events
*******************************

On a coupon payment date of the bond instrument above, the issuer will need to lifecycle the instrument.
This will result in a lifecycle effect for the coupon, which can be cash settled.
This is described in detail in :doc:`Getting Started: Lifecycling <../getting-started/lifecycling>`.

How to redeem a generic instrument
**********************************

On the redemption date, both the last coupon and the redemption amount with be paid.
This is processed in the same way as a single coupon payment described above.
