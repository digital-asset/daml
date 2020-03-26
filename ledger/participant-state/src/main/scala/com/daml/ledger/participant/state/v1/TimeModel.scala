// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time.{Duration, Instant}
import scala.util.Try

/**
  * The ledger time model and associated validations. Some values are given by constructor args; others are derived.
  *
  * @param minTransactionLatency The expected minimum latency of a transaction.
  * @param maxClockSkew          The maximum allowed clock skew between the ledger and clients.
  * @param maxTtl                The maximum allowed time to live for a transaction.
  *                              Must be greater than the derived minimum time to live.
  *
  * @param avgTransactionLatency The expected average latency of a transaction, i.e., the average time
  *                              from submitting the transaction to a [[WriteService]] and the transaction
  *                              being assigned a record time.
  * @param minSkew               The minimimum skew between ledger time and record time: lt_TX >= rt_TX - minSkew
  * @param maxSkew               The maximum skew between ledger time and record time: lt_TX <= rt_TX + maxSkew
  *
  * @throws IllegalArgumentException if the parameters aren't valid
  */
case class TimeModel private (
    minTransactionLatency: Duration,
    maxClockSkew: Duration,
    maxTtl: Duration,
    avgTransactionLatency: Duration,
    minSkew: Duration,
    maxSkew: Duration,
) {

  /**
    * The minimum time to live for a transaction. Equal to the minimum transaction latency plus the maximum clock skew.
    */
  val minTtl: Duration = minTransactionLatency.plus(maxClockSkew)

  /**
    * The maximum window after the current time when transaction ledger effective times will be accepted.
    * Currently equal to the max clock skew.
    * <p/>
    * The corresponding past acceptance window is given by the command's TTL, and thus bounded inclusive by [[maxTtl]].
    */
  val futureAcceptanceWindow: Duration = maxClockSkew

  // TODO(RA) Old ledger time model, remove.
  def checkTtl(givenLedgerEffectiveTime: Instant, givenMaximumRecordTime: Instant): Boolean = {
    val givenTtl = Duration.between(givenLedgerEffectiveTime, givenMaximumRecordTime)
    !givenTtl.minus(minTtl).isNegative && !maxTtl.minus(givenTtl).isNegative
  }

  // TODO(RA) Old ledger time model, remove.
  def checkLet(
      currentTime: Instant,
      givenLedgerEffectiveTime: Instant,
      givenMaximumRecordTime: Instant): Boolean = {
    // Note that, contrary to the documented spec, the record time of a transaction is when it's sequenced.
    // It turns out this isn't a problem for the participant or the sandbox,
    // and MRT seems to be going away in Sirius anyway, so I've left it as is.
    val lowerBound = givenLedgerEffectiveTime.minus(futureAcceptanceWindow)
    !currentTime.isBefore(lowerBound) && !currentTime.isAfter(givenMaximumRecordTime)
  }

  /**
    * Verifies whether the given ledger time and record time are valid under the ledger time model.
    * In particular, checks the skew condition: rt_TX - s_min <= lt_TX <= rt_TX + s_max.
    */
  def checkTime(
      ledgerTime: Instant,
      recordTime: Instant
  ): Either[String, Unit] = {
    val lowerBound = recordTime.minus(minSkew)
    val upperBound = recordTime.plus(maxSkew)
    if (ledgerTime.isBefore(lowerBound) || ledgerTime.isAfter(upperBound))
      Left(s"Ledger time $ledgerTime outside of range [$lowerBound, $upperBound]")
    else
      Right(())
  }

}

object TimeModel {

  /**
    * A default TimeModel that's reasonable for a test or sandbox ledger application.
    * Serious applications (viz. ledger) should probably specify their own TimeModel.
    */
  val reasonableDefault: TimeModel =
    TimeModel(
      minTransactionLatency = Duration.ofSeconds(1L),
      maxClockSkew = Duration.ofSeconds(1L),
      maxTtl = Duration.ofSeconds(30L),
      avgTransactionLatency = Duration.ofSeconds(0L),
      minSkew = Duration.ofSeconds(30L),
      maxSkew = Duration.ofSeconds(30L),
    ).get

  def apply(
      minTransactionLatency: Duration,
      maxClockSkew: Duration,
      maxTtl: Duration,
      avgTransactionLatency: Duration,
      minSkew: Duration,
      maxSkew: Duration): Try[TimeModel] = Try {
    require(!minTransactionLatency.isNegative, "Negative min transaction latency")
    require(!maxTtl.isNegative, "Negative max TTL")
    require(!maxClockSkew.isNegative, "Negative max clock skew")
    require(!maxTtl.minus(maxClockSkew).isNegative, "Max TTL must be greater than max clock skew")
    require(!avgTransactionLatency.isNegative, "Negative average transaction latency")
    require(!minSkew.isNegative, "Negative min skew")
    require(!maxSkew.isNegative, "Negative max skew")
    new TimeModel(
      minTransactionLatency,
      maxClockSkew,
      maxTtl,
      avgTransactionLatency,
      minSkew,
      maxSkew)
  }
}
