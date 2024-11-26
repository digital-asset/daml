// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.leaders.SimpleLeaderSelectionPolicy
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.SelfEnv
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

      epochState.isEpochComplete shouldBe false

      List(1L, 6L, 3L, 4L, BlockNumber.First, 5L).foreach { blockNumber =>
        epochState.confirmBlockCompleted(
          BlockMetadata.mk(EpochNumber.First, blockNumber),
          Seq.empty,
        )
        epochState.isEpochComplete shouldBe false
      }
      epochState.confirmBlockCompleted(BlockMetadata.mk(EpochNumber.First, 2L), Seq.empty)

      epochState.isEpochComplete shouldBe true
    }
  }
}

object EpochStateTest {
  private val myId = fakeSequencerId("self")
  private val otherPeers: Set[SequencerId] = (1 to 3).map { index =>
    fakeSequencerId(s"peer$index")
  }.toSet
}
