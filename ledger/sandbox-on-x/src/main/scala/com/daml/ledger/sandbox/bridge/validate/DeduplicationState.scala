// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.ledger.participant.state.v2.ChangeId
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.bridge.validate.DeduplicationState.DeduplicationQueue
import com.daml.lf.data.Time

import java.time.Duration
import scala.collection.immutable.VectorMap

case class DeduplicationState private (
    private[validate] val deduplicationQueue: DeduplicationQueue,
    private val maxDeduplicationDuration: Duration,
    private val currentTime: () => Time.Timestamp,
    private val bridgeMetrics: BridgeMetrics,
) {

  def deduplicate(
      changeId: ChangeId,
      commandDeduplicationDuration: Duration,
  ): (DeduplicationState, Boolean) = {
    bridgeMetrics.SequencerState.deduplicationQueueLength.update(deduplicationQueue.size)
    if (commandDeduplicationDuration.compareTo(maxDeduplicationDuration) > 0)
      throw new RuntimeException(
        s"Cannot deduplicate for a period ($commandDeduplicationDuration) longer than the max deduplication duration ($maxDeduplicationDuration)."
      )
    else {
      val now = currentTime()
      val expiredTimestamp = expiredThreshold(maxDeduplicationDuration, now)

      val queueAfterEvictions = deduplicationQueue.dropWhile(_._2 <= expiredTimestamp)

      val isDuplicateChangeId = queueAfterEvictions
        .get(changeId)
        .exists(_ > expiredThreshold(commandDeduplicationDuration, now))

      if (isDuplicateChangeId)
        copy(deduplicationQueue = queueAfterEvictions) -> true
      else
        copy(deduplicationQueue = queueAfterEvictions.updated(changeId, now)) -> false
    }
  }

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
      bridgeMetrics: BridgeMetrics,
  ): DeduplicationState =
    DeduplicationState(
      deduplicationQueue = VectorMap.empty,
      maxDeduplicationDuration = deduplicationDuration,
      currentTime = currentTime,
      bridgeMetrics = bridgeMetrics,
    )
}
