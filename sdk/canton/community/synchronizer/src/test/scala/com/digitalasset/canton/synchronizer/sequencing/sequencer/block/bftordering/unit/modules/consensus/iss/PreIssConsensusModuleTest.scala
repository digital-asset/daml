// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.metrics.{BftOrderingMetrics, SequencerMetrics}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.*
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Segment
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.EpochInProgress
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpoch
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.time.SimClock
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class PreIssConsensusModuleTest
    extends AsyncWordSpec
    with BftSequencerBaseTest
    with HasExecutionContext {

  import PreIssConsensusModuleTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  private implicit val context: IgnoringUnitTestContext[Consensus.Message[IgnoringUnitTestEnv]] =
    IgnoringUnitTestContext()

  "PreIssConsensusModule" should {
    "set up the epoch store and state correctly" in {
      Table(
        (
          "latest completed epoch",
          "latest epoch",
          "expected epoch info in state",
        ),
        (GenesisEpoch, GenesisEpoch, GenesisEpoch.info),
        (GenesisEpoch, anEpoch, anEpoch.info),
        (anEpoch, anEpoch, anEpoch.info),
        (anEpoch.copy(lastBlockCommits = someLastBlockCommits), anEpoch, anEpoch.info),
      ).forEvery { (latestCompletedEpoch, latestEpoch, expectedEpochInfoInState) =>
        val epochStore = mock[EpochStore[IgnoringUnitTestEnv]]
        when(epochStore.latestEpoch(includeInProgress = false)).thenReturn(() =>
          latestCompletedEpoch
        )
        when(epochStore.latestEpoch(includeInProgress = true)).thenReturn(() => latestEpoch)
        when(epochStore.loadEpochProgress(latestEpoch.info)).thenReturn(() =>
          EpochStore.EpochInProgress(Seq.empty, Seq.empty)
        )
        val preIssConsensusModule = createPreIssConsensusModule(epochStore)
        val (epochState, lastCompletedEpochRestored) =
          preIssConsensusModule.restoreEpochStateFromDB()

        verify(epochStore).latestEpoch(includeInProgress = true)
        verify(epochStore).latestEpoch(includeInProgress = false)
        verify(epochStore).loadEpochProgress(latestEpoch.info)

        lastCompletedEpochRestored shouldBe latestCompletedEpoch
        epochState.epoch.info shouldBe expectedEpochInfoInState
        epochState
          .segmentModuleRefFactory(
            new SegmentState(
              Segment(selfId, NonEmpty(Seq, BlockNumber.First)), // fake
              epochState.epoch.info.number,
              epochState.epoch.membership,
              epochState.epoch.leaders,
              clock,
              completedBlocks = Seq.empty,
              fail(_),
              mock[BftOrderingMetrics],
              loggerFactory,
            )(MetricsContext.Empty),
            mock[EpochMetricsAccumulator],
          )
          .asInstanceOf[IgnoringSegmentModuleRef[IgnoringUnitTestEnv]]
          .latestCompletedEpochLastCommits shouldBe latestCompletedEpoch.lastBlockCommits
      }
    }
  }

  private def createPreIssConsensusModule(
      epochStore: EpochStore[IgnoringUnitTestEnv]
  ): PreIssConsensusModule[IgnoringUnitTestEnv] =
    new PreIssConsensusModule[IgnoringUnitTestEnv](
      initialMembership = Membership(selfId),
      fakeCryptoProvider,
      epochLength,
      epochStore,
      None,
      clock,
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      new SegmentModuleRefFactory[IgnoringUnitTestEnv] {
        override def apply(
            context: IgnoringUnitTestContext[Consensus.Message[IgnoringUnitTestEnv]],
            epoch: EpochState.Epoch,
            cryptoProvider: CryptoProvider[IgnoringUnitTestEnv],
            latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
            epochInProgress: EpochInProgress,
        )(
            segmentState: SegmentState,
            metricsAccumulator: EpochMetricsAccumulator,
        ): IgnoringSegmentModuleRef[ConsensusSegment.Message] =
          new IgnoringSegmentModuleRef(latestCompletedEpochLastCommits)
      },
      new ConsensusModuleDependencies[IgnoringUnitTestEnv](
        fakeModuleExpectingSilence,
        fakeModuleExpectingSilence,
        fakeModuleExpectingSilence,
      ),
      loggerFactory,
      timeouts,
    )(MetricsContext.Empty)
}

object PreIssConsensusModuleTest {

  private val epochLength = DefaultEpochLength
  private val selfId = fakeSequencerId("self")
  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))
  private val anEpoch =
    EpochStore.Epoch(
      EpochInfo(
        EpochNumber.First,
        BlockNumber.First,
        EpochLength(0),
        TopologyActivationTime(aTimestamp),
      ),
      lastBlockCommits = Seq.empty,
    )
  private val someLastBlockCommits = Seq(
    Commit
      .create(
        BlockMetadata(EpochNumber.First, BlockNumber.First),
        ViewNumber.First,
        Hash.digest(
          HashPurpose.BftOrderingPbftBlock,
          ByteString.EMPTY,
          HashAlgorithm.Sha256,
        ),
        CantonTimestamp.Epoch,
        selfId,
      )
      .fakeSign
  )

  final class IgnoringSegmentModuleRef[-MessageT](
      val latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]]
  ) extends IgnoringModuleRef[MessageT] {
    override def asyncSend(msg: MessageT): Unit = ()
  }
}
