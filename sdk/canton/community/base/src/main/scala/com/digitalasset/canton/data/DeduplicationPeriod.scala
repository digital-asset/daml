// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import com.digitalasset.daml.lf.data.Time.Timestamp

import java.time.Duration
import scala.util.{Failure, Success, Try}

/** Specifies the deduplication period for a command submission.
  * Note that we would like to keep this easily extensible to support offsets and absolute
  * timestamps, hence the usage of a trait here.
  *
  * @see com.digitalasset.canton.ledger.participant.state.v2.ReadService.stateUpdates for the deduplication guarantee
  */
sealed trait DeduplicationPeriod extends Product with Serializable

object DeduplicationPeriod {

  /** Transforms the `period` into a [[com.digitalasset.daml.lf.data.Time.Timestamp]] to be used for deduplication into the future(deduplicateUntil).
    * Only used for backwards compatibility
    * @param time The time to use for calculating the [[com.digitalasset.daml.lf.data.Time.Timestamp]]. It can either be submission time or current time, based on usage
    * @param period The deduplication period
    */
  def deduplicateUntil(
      time: Timestamp,
      period: DeduplicationPeriod,
  ): Try[Timestamp] = period match {
    case DeduplicationDuration(duration) =>
      Success(time.addMicros(duration.toNanos / 1000))
    case DeduplicationOffset(_) =>
      Failure(
        new NotImplementedError("Offset deduplication is not supported")
      )
  }

  /** The length of the deduplication window, which ends when the [[com.digitalasset.canton.ledger.participant.state.WriteService]] or underlying Daml ledger processes
    * the command submission.
    *
    * When used in [[com.digitalasset.canton.ledger.participant.state.SubmitterInfo]], the window is measured on some unspecified clock on the participant or the Daml ledger.
    * When used in [[com.digitalasset.canton.ledger.participant.state.CompletionInfo]], the window is measured in record time.
    *
    * @throws java.lang.IllegalArgumentException if the `duration` is negative or uses finer than microsecond precision
    */
  final case class DeduplicationDuration(duration: Duration) extends DeduplicationPeriod {
    require(!duration.isNegative, s"The deduplication window must not be negative: $duration")
    require(
      duration.getNano % 1000 == 0,
      s"The deduplication window must not use nanosecond precision: $duration",
    )
  }

  /** The `offset` defines the start of the deduplication period (exclusive). */
  final case class DeduplicationOffset(offset: Offset) extends DeduplicationPeriod

  implicit val `DeduplicationPeriod to LoggingValue`: ToLoggingValue[DeduplicationPeriod] = {
    case DeduplicationDuration(duration) =>
      LoggingValue.Nested.fromEntries("duration" -> duration)
    case DeduplicationOffset(offset) =>
      LoggingValue.Nested.fromEntries("offset" -> offset)
  }
}
