// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import java.time.{Duration, Instant}

/** Specifies the deduplication period for a command submission.
  *
  * @see com.daml.ledger.participant.state.v1.ReadService.stateUpdates for the deduplication guarantee
  */
sealed trait DeduplicationPeriod extends Product with Serializable

object DeduplicationPeriod {

  /** The `offset` defines the start of the deduplication period. */
  case class DeduplicationOffset(offset: Offset) extends DeduplicationPeriod

  /** The instant `since` specifies the point in time where deduplication starts.
    * When used in [[SubmitterInfo]], this point in time is measured on some unspecified clock on the participant or the Daml ledger.
    * When used in [[CompletionInfo]], this point in time is measured in record time.
    */
  case class DeduplicationTimepoint(since: Instant) extends DeduplicationPeriod

  /** The length of the deduplication window, which ends when the [[WriteService]] or underlying Daml ledger processes
    * the command submission.
    *
    * When used in [[SubmitterInfo]], the window is measured on some unspecified clock on the participant or the Daml ledger.
    * When used in [[CompletionInfo]], the window is measured in record time.
    *
    * @throws java.lang.IllegalArgumentException if the `duration` is negative
    */
  case class DeduplicationDuration(duration: Duration) extends DeduplicationPeriod {
    require(!duration.isNegative, s"The deduplication window must not be negative: $duration")
  }
}
