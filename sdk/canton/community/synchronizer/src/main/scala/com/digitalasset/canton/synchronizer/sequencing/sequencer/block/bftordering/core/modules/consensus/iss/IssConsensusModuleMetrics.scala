// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricHandle.Gauge
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.topology.SequencerId

import java.time.{Duration, Instant}

import EpochState.Epoch

private[iss] object IssConsensusModuleMetrics {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var lastConsensusCommitInstant: Option[Instant] = None

  def emitConsensusLatencyStats(
      metrics: BftOrderingMetrics
  )(implicit mc: MetricsContext): Unit = {
    val now = Instant.now()
    lastConsensusCommitInstant.foreach { instant =>
      val duration = Duration.between(instant, now)
      metrics.consensus.commitLatency.update(duration)
    }
    lastConsensusCommitInstant = Some(now)
  }

  def emitEpochStats(
      metrics: BftOrderingMetrics,
      epoch: EpochInfo,
      prevEpoch: Epoch,
      prevEpochViewsCount: Long,
      prevEpochPrepareVotes: Map[SequencerId, Long],
      prevEpochCommitVotes: Map[SequencerId, Long],
  )(implicit mc: MetricsContext): Unit = {
    val totalConsensusStageVotes =
      totalConsensusStageVotesInEpoch(
        prevEpoch.membership.orderingTopology.peers.size,
        prevEpoch.info.length,
        prevEpoch.leaders.size,
        prevEpochViewsCount,
      )

    metrics.consensus.epoch.updateValue(epoch.number)
    metrics.consensus.epochLength.updateValue(epoch.length)

    emitVoteStats(
      totalConsensusStageVotes,
      prevEpoch.membership.orderingTopology.peers.size,
      VoteStatsSpec(metrics.consensus.votes.prepareVotesPercent, prevEpochPrepareVotes),
      VoteStatsSpec(metrics.consensus.votes.commitVotesPercent, prevEpochCommitVotes),
    )
  }

  private[iss] def totalConsensusStageVotesInEpoch(
      peers: Int,
      epochLength: EpochLength,
      segmentLeaders: Int,
      viewsCount: Long,
  ): Long = {
    val happyPathVotes = epochLength * peers
    val viewChanges = viewsCount - segmentLeaders
    // TODO(#20378): breaks when we introduce commit certificates, revisit then
    val additionalVotesDueToViewChanges = peers * viewChanges
    happyPathVotes + additionalVotesDueToViewChanges
  }

  def emitNonCompliance(metrics: BftOrderingMetrics)(
      from: SequencerId,
      epoch: EpochNumber,
      view: ViewNumber,
      block: BlockNumber,
      kind: metrics.security.noncompliant.labels.violationType.values.ViolationTypeValue,
  )(implicit mc: MetricsContext): Unit =
    metrics.security.noncompliant.behavior.mark()(
      mc.withExtraLabels(
        metrics.security.noncompliant.labels.Sequencer -> from.toProtoPrimitive,
        metrics.security.noncompliant.labels.Epoch -> epoch.toString,
        metrics.security.noncompliant.labels.View -> view.toString,
        metrics.security.noncompliant.labels.Block -> block.toString,
        metrics.security.noncompliant.labels.violationType.Key -> kind,
      )
    )

  private final case class VoteStatsSpec(
      getGauge: SequencerId => Gauge[Double],
      getVotes: Map[SequencerId, Long],
  )

  private def emitVoteStats(
      totalConsensusStageVotes: Long,
      peersCount: Int,
      voteStatsSpecs: VoteStatsSpec*
  )(implicit mc: MetricsContext): Unit = {
    val singleNodeConsensusStageVotes = totalConsensusStageVotes.toDouble / peersCount
    voteStatsSpecs.foreach { case VoteStatsSpec(getGauge, getVotes) =>
      val allStageVotes = getVotes
      allStageVotes.foreach { case (sequencerId, votes) =>
        getGauge(sequencerId).updateValue(
          votes.toDouble / singleNodeConsensusStageVotes
        )
      }
    }
  }
}
