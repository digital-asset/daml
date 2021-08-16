// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import java.time.Duration

import com.daml.ledger.offset.Offset
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}

/** Specifies the deduplication period for a command submission.
  * Note that we would like to keep this easily extensible to support offsets and absolute
  * timestamps, hence the usage of a trait here.
  *
  * @see com.daml.ledger.participant.state.v2.ReadService.stateUpdates for the deduplication guarantee
  */
sealed trait DeduplicationPeriod extends Product with Serializable

object DeduplicationPeriod {

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
  }

  /** The `offset` defines the start of the deduplication period. */
  final case class DeduplicationOffset(offset: Offset) extends DeduplicationPeriod

  implicit val `DeduplicationPeriod to LoggingValue`: ToLoggingValue[DeduplicationPeriod] = {
    case DeduplicationDuration(duration) =>
      LoggingValue.Nested.fromEntries("duration" -> duration)
    case DeduplicationOffset(offset) =>
      LoggingValue.Nested.fromEntries("offset" -> offset)
  }
}
