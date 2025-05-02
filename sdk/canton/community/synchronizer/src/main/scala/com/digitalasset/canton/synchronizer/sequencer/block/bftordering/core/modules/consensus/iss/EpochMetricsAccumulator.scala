// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId

import java.util.concurrent.atomic.AtomicLong
import scala.collection.concurrent.TrieMap

class EpochMetricsAccumulator {
  private val commitVotesAccumulator: TrieMap[BftNodeId, Long] = TrieMap.empty
  private val prepareVotesAccumulator: TrieMap[BftNodeId, Long] = TrieMap.empty
  private val viewsAccumulator = new AtomicLong(0L)
  private val discardedMessagesAccumulator = new AtomicLong(0L)
  private val retransmittedMessagesAccumulator = new AtomicLong(0L)
  private val retransmittedCommitCertificatesAccumulator = new AtomicLong(0L)

  def prepareVotes: Map[BftNodeId, Long] = prepareVotesAccumulator.toMap
  def commitVotes: Map[BftNodeId, Long] = commitVotesAccumulator.toMap
  def viewsCount: Long = viewsAccumulator.get()
  def discardedMessages: Long = discardedMessagesAccumulator.get()
  def retransmittedMessages: Long = retransmittedMessagesAccumulator.get()
  def retransmittedCommitCertificates: Long = retransmittedCommitCertificatesAccumulator.get()

  private def accumulate(accumulator: TrieMap[BftNodeId, Long])(
      values: Map[BftNodeId, Long]
  ): Unit =
    values.foreach { case (node, newVotes) =>
      accumulator
        .updateWith(node) {
          case Some(votes) => Some(votes + newVotes)
          case None => Some(newVotes)
        }
        .discard
    }

  def accumulate(
      views: Long,
      commits: Map[BftNodeId, Long],
      prepares: Map[BftNodeId, Long],
      discardedMessageCount: Int,
      retransmittedMessagesCount: Int,
      retransmittedCommitCertificatesCount: Int,
  ): Unit = {
    viewsAccumulator.addAndGet(views).discard
    accumulate(commitVotesAccumulator)(commits)
    accumulate(prepareVotesAccumulator)(prepares)
    discardedMessagesAccumulator.addAndGet(discardedMessageCount.toLong).discard
    retransmittedMessagesAccumulator.addAndGet(retransmittedMessagesCount.toLong).discard
    retransmittedCommitCertificatesAccumulator
      .addAndGet(retransmittedCommitCertificatesCount.toLong)
      .discard
  }

}
