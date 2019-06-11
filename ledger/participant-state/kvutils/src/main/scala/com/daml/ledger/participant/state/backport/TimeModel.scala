// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.backport

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.v1.{
  TimeModel => ITimeModel,
  TimeModelChecker => ITimeModelChecker
}

import scala.util.Try

/**
  * The ledger time model and associated validations. Some values are given by constructor args; others are derived.
  *
  * @param minTransactionLatency The expected minimum latency of a transaction.
  * @param maxClockSkew          The maximum allowed clock skew between the ledger and clients.
  * @param maxTtl                The maximum allowed time to live for a transaction.
  *                              Must be greater than the derived minimum time to live.
  * @throws IllegalArgumentException if the parameters aren't valid
  */
class TimeModel private (
    val minTransactionLatency: Duration,
    val maxClockSkew: Duration,
    val maxTtl: Duration)
    extends ITimeModel {

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

}

object TimeModel {

  /**
    * A default TimeModel that's reasonable for a test or sandbox ledger application.
    * Serious applications (viz. ledger) should probably specify their own TimeModel.
    */
  val reasonableDefault: TimeModel =
    TimeModel(Duration.ofSeconds(1L), Duration.ofSeconds(1L), Duration.ofSeconds(30L)).get

  def apply(
      minTransactionLatency: Duration,
      maxClockSkew: Duration,
      maxTtl: Duration): Try[TimeModel] = Try {
    require(!minTransactionLatency.isNegative, "Negative min transaction latency")
    require(!maxTtl.isNegative, "Negative max TTL")
    require(!maxClockSkew.isNegative, "Negative max clock skew")
    require(!maxTtl.minus(maxClockSkew).isNegative, "Max TTL must be greater than max clock skew")
    new TimeModel(minTransactionLatency, maxClockSkew, maxTtl)
  }
}

case class TimeModelChecker(timeModel: ITimeModel) extends ITimeModelChecker {

  import timeModel._

  override def checkTtl(
      givenLedgerEffectiveTime: Instant,
      givenMaximumRecordTime: Instant): Boolean = {
    val givenTtl = Duration.between(givenLedgerEffectiveTime, givenMaximumRecordTime)
    !givenTtl.minus(minTtl).isNegative && !maxTtl.minus(givenTtl).isNegative
  }

  override def checkLet(
      currentTime: Instant,
      givenLedgerEffectiveTime: Instant,
      givenMaximumRecordTime: Instant): Boolean = {
    // Note that, contrary to the documented spec, the record time of a transaction is when it's sequenced.
    // It turns out this isn't a problem for the participant or the sandbox,
    // and MRT seems to be going away in Sirius anyway, so I've left it as is.
    val lowerBound = givenLedgerEffectiveTime.minus(futureAcceptanceWindow)
    !currentTime.isBefore(lowerBound) && !currentTime.isAfter(givenMaximumRecordTime)
  }
}
