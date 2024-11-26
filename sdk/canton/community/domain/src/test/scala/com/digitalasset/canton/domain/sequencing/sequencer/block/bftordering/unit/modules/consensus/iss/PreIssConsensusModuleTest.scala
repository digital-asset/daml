// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultEpochLength
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.EpochInProgress
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class PreIssConsensusModuleTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  import PreIssConsensusModuleTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  private implicit val context: IgnoringUnitTestContext[Consensus.Message[IgnoringUnitTestEnv]] =
    IgnoringUnitTestContext()

  "PreIssConsensusModule" should {
    "set up the epoch store and state correctly" in {

      val anEpoch =
        EpochStore.Epoch(
          EpochInfo(
            EpochNumber.First,
            BlockNumber.First,
            EpochLength(0),
            TopologyActivationTime(aTimestamp),
          ),
          Seq.empty,
        )

      Table(
        (
          "latest completed epoch",
          "latest epoch",
          "expected epoch info in state",
        ),
        (GenesisEpoch, GenesisEpoch, GenesisEpoch.info),
        (GenesisEpoch, anEpoch, anEpoch.info),
        (anEpoch, anEpoch, anEpoch.info),
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
        ): IgnoringModuleRef[ConsensusSegment.Message] = new IgnoringModuleRef
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
}
