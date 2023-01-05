// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import java.time.Duration

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}

import scala.util.{Failure, Success, Try}

/** Specifies the deduplication period for a command submission.
  * Note that we would like to keep this easily extensible to support offsets and absolute
  * timestamps, hence the usage of a trait here.
  *
  * @see com.daml.ledger.participant.state.v2.ReadService.stateUpdates for the deduplication guarantee
  */
sealed trait DeduplicationPeriod extends Product with Serializable

object DeduplicationPeriod {

  /** Transforms the [[period]] into a [[Timestamp]] to be used for deduplication into the future(deduplicateUntil).
    * Only used for backwards compatibility
    * @param time The time to use for calculating the [[Timestamp]]. It can either be submission time or current time, based on usage
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

  /** Computes deduplication duration as the duration `time + minSkew - deduplicationStart`.
    * We measure `deduplicationStart` on the ledger’s clock, and thus
    *    we need to add the minSkew to compensate for the maximal skew that the participant might be behind the ledger’s clock.
    * @param time submission time or current time
    * @param deduplicationStart the [[Timestamp]] from where we should start deduplication, must be < than time
    * @param minSkew the minimum skew as specified by the current ledger time model
    */
  def deduplicationDurationFromTime(
      time: Timestamp,
      deduplicationStart: Timestamp,
      minSkew: Duration,
  ): Duration = {
    assert(deduplicationStart < time, "Deduplication must start in the past")
    Duration.between(
      deduplicationStart.toInstant,
      time.toInstant.plus(minSkew),
    )
  }

  /** The length of the deduplication window, which ends when the [[WriteService]] or underlying Daml ledger processes
    * the command submission.
    *
    * When used in [[SubmitterInfo]], the window is measured on some unspecified clock on the participant or the Daml ledger.
    * When used in [[CompletionInfo]], the window is measured in record time.
    *
    * @throws java.lang.IllegalArgumentException if the `duration` is negative
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
