// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.leaders.SimpleLeaderSelectionPolicy
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.SelfEnv
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.unused

class EpochStateTest extends AsyncWordSpec with BaseTest {

  import EpochStateTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  "EpochState" should {

    "return the last block's completed commit messages" when {
      "created with the last block already completed" in {
        val epochInfo =
          EpochInfo.mk(
            number = EpochNumber.First,
            startBlockNumber = BlockNumber.First,
            length = 7,
          )
        val membership = Membership(myId, otherPeers)
        val epoch =
          Epoch(
            epochInfo,
            currentMembership = membership,
            previousMembership = membership, // Not relevant for the test
            SimpleLeaderSelectionPolicy,
          )
        val epochState =
          new EpochState[SelfEnv](
            epoch,
            clock,
            fail(_),
            metrics,
            segmentModuleRefFactory,
            loggerFactory = loggerFactory,
            timeouts = timeouts,
            completedBlocks = Seq(
              EpochStore.Block(
                EpochNumber.First,
                BlockNumber(6L),
                CommitCertificate(pp, Seq(commit)),
              )
            ),
          )

        epochState.lastBlockCommitMessages should contain only commit
      }
    }

    "complete epoch once all blocks are stored" in {
      val epochInfo =
        EpochInfo.mk(
          number = EpochNumber.First,
          startBlockNumber = BlockNumber.First,
          length = 7,
        )
      val membership = Membership(myId, otherPeers)
      val epoch =
        Epoch(
          epochInfo,
          currentMembership = membership,
          previousMembership = membership, // Not relevant for the test
          SimpleLeaderSelectionPolicy,
        )

      val epochState =
        new EpochState[SelfEnv](
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

  private implicit val mc: MetricsContext = MetricsContext.Empty
  private val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

  private val myId = fakeSequencerId("self")
  private val otherPeers: Set[SequencerId] = (1 to 3).map { index =>
    fakeSequencerId(s"peer$index")
  }.toSet

  private val pp =
    PrePrepare
      .create(
        BlockMetadata.mk(EpochNumber.First, BlockNumber.First),
        ViewNumber.First,
        CantonTimestamp.Epoch,
        OrderingBlock.empty,
        CanonicalCommitSet.empty,
        from = myId,
      )
      .fakeSign

  private val commit =
    Commit
      .create(
        BlockMetadata(EpochNumber.First, BlockNumber(6L)),
        ViewNumber.First,
        Hash.digest(
          HashPurpose.BftOrderingPbftBlock,
          ByteString.EMPTY,
          HashAlgorithm.Sha256,
        ),
        CantonTimestamp.Epoch,
        from = myId,
      )
      .fakeSign

  private def segmentModuleRefFactory(
      segmentState: SegmentState,
      @unused _epochMetricsAccumulator: EpochMetricsAccumulator,
  )(implicit traceContext: TraceContext): ModuleRef[ConsensusSegment.Message] = {
    case pbftEvent: ConsensusSegment.ConsensusMessage.PbftEvent =>
      segmentState.processEvent(pbftEvent)
    case _ => ()
  }
}
