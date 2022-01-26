// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.ledger.participant.state.v2.ChangeId
import com.daml.ledger.sandbox.bridge.validate.DeduplicationState.DeduplicationQueue
import com.daml.lf.data.Time

import java.time.Duration
import scala.collection.immutable.VectorMap

case class DeduplicationState private (
    private[validate] val deduplicationQueue: DeduplicationQueue,
    deduplicationDuration: Duration,
    currentTime: () => Time.Timestamp,
) {

  def newTransactionAccepted(changeId: ChangeId): DeduplicationState = {
    val now = currentTime()
    val expiredTimestamp = expiredThreshold(deduplicationDuration, now)

    val updatedQueue =
      deduplicationQueue
        .updated(changeId, now)
        .dropWhile(_._2 <= expiredTimestamp)

    DeduplicationState(
      deduplicationQueue = updatedQueue,
      deduplicationDuration = deduplicationDuration,
      currentTime = currentTime,
    )
  }

  def isDuplicate(changeId: ChangeId, commandDeduplicationDuration: Duration): Boolean =
    deduplicationQueue
      .get(changeId)
      .exists(_ > expiredThreshold(commandDeduplicationDuration, currentTime()))

  private def expiredThreshold(
      deduplicationDuration: Duration,
      now: Time.Timestamp,
  ): Time.Timestamp =
    now.subtract(deduplicationDuration)
}

object DeduplicationState {
  private[sandbox] type DeduplicationQueue = VectorMap[ChangeId, Time.Timestamp]

  private[validate] def empty(
      deduplicationDuration: Duration,
      currentTime: () => Time.Timestamp,
  ): DeduplicationState =
    DeduplicationState(
      deduplicationQueue = VectorMap.empty,
      deduplicationDuration = deduplicationDuration,
      currentTime = currentTime,
    )
}
