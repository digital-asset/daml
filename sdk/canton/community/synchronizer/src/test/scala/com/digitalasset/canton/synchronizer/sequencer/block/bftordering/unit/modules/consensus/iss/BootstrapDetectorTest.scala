// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.DefaultEpochLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.BootstrapDetector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  Genesis,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.{
  NodeActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import org.scalatest.wordspec.AnyWordSpec

import BootstrapDetector.BootstrapKind

class BootstrapDetectorTest extends AnyWordSpec with BftSequencerBaseTest {

  import BootstrapDetectorTest.*

  "detect bootstrap kind" in {
    forAll(
      Table[Option[SequencerSnapshotAdditionalInfo], Membership, EpochStore.Epoch, BootstrapKind](
        (
          "sequencer snapshot additional info",
          "membership",
          "latest completed epoch",
          "expected should state transfer true/false",
        ),
        // No sequencer snapshot
        (
          None,
          aMembershipWith2Nodes,
          Genesis.GenesisEpoch,
          BootstrapKind.RegularStartup,
        ),
        // Only 1 node
        (
          Some(aSequencerSnapshot),
          Membership.forTesting(myId),
          Genesis.GenesisEpoch,
          BootstrapKind.RegularStartup,
        ),
        // Non-zero starting epoch
        (
          Some(aSequencerSnapshot),
          aMembershipWith2Nodes,
          Epoch(
            EpochInfo(
              EpochNumber(7L),
              BlockNumber(70L),
              EpochLength(10L),
              Genesis.GenesisTopologyActivationTime,
            ),
            lastBlockCommits = Seq.empty,
          ),
          BootstrapKind.RegularStartup,
        ),
        // Onboarding
        (
          Some(aSequencerSnapshot),
          aMembershipWith2Nodes,
          Genesis.GenesisEpoch,
          BootstrapKind.Onboarding(
            EpochInfo(
              EpochNumber(1500L),
              BlockNumber(15000L),
              DefaultEpochLength,
              topologyActivationTime = TopologyActivationTime(CantonTimestamp.MinValue),
            )
          ),
        ),
      )
    ) { (snapshotAdditionalInfo, membership, latestCompletedEpoch, expectedShouldStateTransfer) =>
      BootstrapDetector.detect(
        DefaultEpochLength,
        snapshotAdditionalInfo,
        membership,
        latestCompletedEpoch,
      )(fail(_)) shouldBe expectedShouldStateTransfer
    }
  }

  "fail on missing start epoch number" in {
    a[RuntimeException] shouldBe thrownBy(
      BootstrapDetector.detect(
        DefaultEpochLength,
        Some(SequencerSnapshotAdditionalInfo(Map.empty /* boom! */ )),
        aMembershipWith2Nodes,
        Genesis.GenesisEpoch,
      )(_ => throw new RuntimeException("aborted"))
    )
  }
}

object BootstrapDetectorTest {

  private val myId = BftNodeId("self")
  private val otherId = BftNodeId("other")
  private val aMembershipWith2Nodes =
    Membership.forTesting(myId, Set(otherId))
  private val aSequencerSnapshot = SequencerSnapshotAdditionalInfo(
    // Minimal data required for the test
    Map(
      myId -> NodeActiveAt(
        TopologyActivationTime(CantonTimestamp.Epoch),
        Some(EpochNumber(1500L)),
        firstBlockNumberInStartEpoch = Some(BlockNumber(15000L)),
        startEpochTopologyQueryTimestamp = Some(TopologyActivationTime(CantonTimestamp.MinValue)),
        startEpochCouldAlterOrderingTopology = None,
        previousBftTime = None,
        previousEpochTopologyQueryTimestamp = None,
      )
    )
  )
}
