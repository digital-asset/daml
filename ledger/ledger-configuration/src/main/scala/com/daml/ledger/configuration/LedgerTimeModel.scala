// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.configuration

import java.time.{Duration, Instant}

import com.daml.ledger.configuration.LedgerTimeModel._

import scala.util.Try

/** The ledger time model and associated validations. Some values are given by constructor args;
  * others are derived.
  *
  * @param avgTransactionLatency  The expected average latency of a transaction, i.e., the average
  *                               time from submitting the transaction to a write service and the
  *                               transaction being assigned a record time.
  * @param minSkew                The minimimum skew between ledger time and record time:
  *                               lt_TX >= rt_TX - minSkew
  * @param maxSkew                The maximum skew between ledger time and record time:
  *                               lt_TX <= rt_TX + maxSkew
  * @throws IllegalArgumentException if the parameters aren't valid
  */
case class LedgerTimeModel private (
    avgTransactionLatency: Duration,
    minSkew: Duration,
    maxSkew: Duration,
) {

  /** Verifies whether the given ledger time and record time are valid under the ledger time model.
    * In particular, checks the skew condition: rt_TX - s_min <= lt_TX <= rt_TX + s_max.
    */
  def checkTime(
      ledgerTime: Instant,
      recordTime: Instant,
  ): Either[OutOfRange, Unit] = {
    val lowerBound = minLedgerTime(recordTime)
    val upperBound = maxLedgerTime(recordTime)
    if (ledgerTime.isBefore(lowerBound) || ledgerTime.isAfter(upperBound)) {
      Left(OutOfRange(ledgerTime, lowerBound, upperBound))
    } else {
      Right(())
    }
  }

  private[ledger] def minLedgerTime(recordTime: Instant): Instant =
    recordTime.minus(minSkew)

  private[ledger] def maxLedgerTime(recordTime: Instant): Instant =
    recordTime.plus(maxSkew)

  private[ledger] def minRecordTime(ledgerTime: Instant): Instant =
    ledgerTime.minus(maxSkew)

  private[ledger] def maxRecordTime(ledgerTime: Instant): Instant =
    ledgerTime.plus(minSkew)
}

object LedgerTimeModel {

  /** A default TimeModel that's reasonable for a test or sandbox ledger application.
    * Serious applications (viz. ledger) should probably specify their own TimeModel.
    */
  val reasonableDefault: LedgerTimeModel =
    LedgerTimeModel(
      avgTransactionLatency = Duration.ofSeconds(0L),
      minSkew = Duration.ofSeconds(120L),
      maxSkew = Duration.ofSeconds(120L),
    ).get

  def apply(
      avgTransactionLatency: Duration,
      minSkew: Duration,
      maxSkew: Duration,
  ): Try[LedgerTimeModel] =
    Try {
      require(!avgTransactionLatency.isNegative, "Negative average transaction latency")
      require(!minSkew.isNegative, "Negative min skew")
      require(!maxSkew.isNegative, "Negative max skew")
      new LedgerTimeModel(avgTransactionLatency, minSkew, maxSkew)
    }

  final case class OutOfRange(ledgerTime: Instant, lowerBound: Instant, upperBound: Instant) {
    lazy val message: String =
      s"Ledger time $ledgerTime outside of range [$lowerBound, $upperBound]"
  }

}
