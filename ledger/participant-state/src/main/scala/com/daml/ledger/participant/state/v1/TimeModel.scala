// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time.{Duration, Instant}
import scala.util.Try

/**
  * The ledger time model and associated validations. Some values are given by constructor args; others are derived.
  * @param avgTransactionLatency The expected average latency of a transaction, i.e., the average time
  *                              from submitting the transaction to a [[WriteService]] and the transaction
  *                              being assigned a record time.
  * @param minSkew               The minimimum skew between ledger time and record time: lt_TX >= rt_TX - minSkew
  * @param maxSkew               The maximum skew between ledger time and record time: lt_TX <= rt_TX + maxSkew
  *
  * @throws IllegalArgumentException if the parameters aren't valid
  */
case class TimeModel private (
    avgTransactionLatency: Duration,
    minSkew: Duration,
    maxSkew: Duration,
) {

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
      avgTransactionLatency = Duration.ofSeconds(0L),
      minSkew = Duration.ofSeconds(30L),
      maxSkew = Duration.ofSeconds(30L),
    ).get

  def apply(avgTransactionLatency: Duration, minSkew: Duration, maxSkew: Duration): Try[TimeModel] =
    Try {
      require(!avgTransactionLatency.isNegative, "Negative average transaction latency")
      require(!minSkew.isNegative, "Negative min skew")
      require(!maxSkew.isNegative, "Negative max skew")
      new TimeModel(avgTransactionLatency, minSkew, maxSkew)
    }
}
