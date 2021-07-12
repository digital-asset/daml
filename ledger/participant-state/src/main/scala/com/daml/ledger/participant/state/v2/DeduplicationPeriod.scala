// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.time.Duration

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
}
