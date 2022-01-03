// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.configuration

import java.time.Duration

import com.daml.ledger.configuration.LedgerTimeModel._
import com.daml.lf.data.Time.Timestamp

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
      ledgerTime: Timestamp,
      recordTime: Timestamp,
  ): Either[OutOfRange, Unit] = {
    val lowerBound = minLedgerTime(recordTime)
    val upperBound = maxLedgerTime(recordTime)
    if (ledgerTime < lowerBound || ledgerTime > upperBound) {
      Left(OutOfRange(ledgerTime, lowerBound, upperBound))
    } else {
      Right(())
    }
  }

  private[ledger] def minLedgerTime(recordTime: Timestamp): Timestamp =
    recordTime.subtract(minSkew)

  private[ledger] def maxLedgerTime(recordTime: Timestamp): Timestamp =
    recordTime.add(maxSkew)

  private[ledger] def minRecordTime(ledgerTime: Timestamp): Timestamp =
    ledgerTime.subtract(maxSkew)

  private[ledger] def maxRecordTime(ledgerTime: Timestamp): Timestamp =
    ledgerTime.add(minSkew)
}

object LedgerTimeModel {

  /** A default TimeModel that's reasonable for a test or sandbox ledger application.
    * Serious applications (viz. ledger) should probably specify their own TimeModel.
    */
  val reasonableDefault: LedgerTimeModel =
    LedgerTimeModel(
      avgTransactionLatency = Duration.ofSeconds(0L),
      minSkew = Duration.ofSeconds(30L),
      maxSkew = Duration.ofSeconds(30L),
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

  final case class OutOfRange(ledgerTime: Timestamp, lowerBound: Timestamp, upperBound: Timestamp) {
    lazy val message: String =
      s"Ledger time $ledgerTime outside of range [$lowerBound, $upperBound]"
  }

}
