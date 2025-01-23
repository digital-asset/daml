// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.leaders.SimpleLeaderSelectionPolicy
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.{
  EpochMetricsAccumulator,
  EpochState,
  SegmentState,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.SelfEnv
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SequencerId
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.unused

class EpochStateTest extends AsyncWordSpec with BaseTest {
  import EpochStateTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  private implicit val mc: MetricsContext = MetricsContext.Empty
  private val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

  def segmentModuleRefFactory(
      segmentState: SegmentState,
      @unused _epochMetricsAccumulator: EpochMetricsAccumulator,
  ): ModuleRef[ConsensusSegment.Message] = {
    case pbftEvent: ConsensusSegment.ConsensusMessage.PbftEvent =>
      segmentState.processEvent(pbftEvent)
    case _ => ()
  }

  "EpochState" should {
    "complete epoch once all blocks are stored" in {
      val epochInfo = EpochInfo.mk(
        number = EpochNumber.First,
        startBlockNumber = BlockNumber.First,
        length = 7,
      )
      val epoch =
        Epoch(
          epochInfo,
          Membership(myId, otherPeers),
          SimpleLeaderSelectionPolicy,
        )

      val epochState = new EpochState[SelfEnv](
        epoch,
        clock,
        fail(_),
        metrics,
        segmentModuleRefFactory,
        loggerFactory = loggerFactory,
        timeouts = timeouts,
      )

      epochState.epochCompletionStatus.isComplete shouldBe false

      List(1L, 6L, 3L, 4L, BlockNumber.First, 5L).foreach { blockNumber =>
        epochState.confirmBlockCompleted(
          BlockMetadata.mk(EpochNumber.First, blockNumber),
          CommitCertificate(pp, Seq.empty),
        )
        epochState.epochCompletionStatus.isComplete shouldBe false
      }
      epochState.confirmBlockCompleted(
        BlockMetadata.mk(EpochNumber.First, 2L),
        CommitCertificate(pp, Seq.empty),
      )

      epochState.epochCompletionStatus.isComplete shouldBe true
    }
  }
}

object EpochStateTest {
  private val myId = fakeSequencerId("self")
  private val otherPeers: Set[SequencerId] = (1 to 3).map { index =>
    fakeSequencerId(s"peer$index")
  }.toSet
  private val pp = PrePrepare
    .create(
      BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
      ViewNumber.First,
      CantonTimestamp.Epoch,
      OrderingBlock.empty,
      CanonicalCommitSet.empty,
      from = myId,
    )
    .fakeSign
}
