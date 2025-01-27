// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.topology.SequencerId

import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap

class EpochMetricsAccumulator {
  private val commitVotesAccumulator: TrieMap[SequencerId, Long] = TrieMap.empty
  private val prepareVotesAccumulator: TrieMap[SequencerId, Long] = TrieMap.empty
  private val viewsAccumulator = new AtomicLong(0L)

  def prepareVotes: Map[SequencerId, Long] = prepareVotesAccumulator.toMap
  def commitVotes: Map[SequencerId, Long] = commitVotesAccumulator.toMap
  def viewsCount: Long = viewsAccumulator.get()

  private def accumulate(accumulator: TrieMap[SequencerId, Long])(
      values: Map[SequencerId, Long]
  ): Unit =
    values.foreach { case (sequencerId, newVotes) =>
      accumulator
        .updateWith(sequencerId) {
          case Some(votes) => Some(votes + newVotes)
          case None => Some(newVotes)
        }
        .discard
    }

  def accumulate(
      views: Long,
      commits: Map[SequencerId, Long],
      prepares: Map[SequencerId, Long],
  ): Unit = {
    viewsAccumulator.addAndGet(views).discard
    accumulate(commitVotesAccumulator)(commits)
    accumulate(prepareVotesAccumulator)(prepares)
  }

}
