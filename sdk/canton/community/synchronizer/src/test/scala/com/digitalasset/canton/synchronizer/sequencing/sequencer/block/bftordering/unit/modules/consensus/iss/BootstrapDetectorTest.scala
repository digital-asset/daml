// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.unit.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.BootstrapDetector
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.BootstrapDetector.BootstrapKind
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  Genesis,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.snapshot.{
  PeerActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import org.scalatest.wordspec.AnyWordSpec

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
          Membership(mySequencerId),
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
            lastBlockCommitMessages = Seq.empty,
          ),
          BootstrapKind.RegularStartup,
        ),
        // Onboarding
        (
          Some(aSequencerSnapshot),
          aMembershipWith2Nodes,
          Genesis.GenesisEpoch,
          BootstrapKind.Onboarding(EpochNumber(1500L)),
        ),
      )
    ) { (snapshotAdditionalInfo, membership, latestCompletedEpoch, expectedShouldStateTransfer) =>
      BootstrapDetector.detect(
        snapshotAdditionalInfo,
        membership,
        latestCompletedEpoch,
      )(fail(_)) shouldBe expectedShouldStateTransfer
    }
  }

  "fail on missing start epoch number" in {
    a[RuntimeException] shouldBe thrownBy(
      BootstrapDetector.detect(
        Some(SequencerSnapshotAdditionalInfo(Map.empty /* boom! */ )),
        aMembershipWith2Nodes,
        Genesis.GenesisEpoch,
      )(_ => throw new RuntimeException("aborted"))
    )
  }
}

object BootstrapDetectorTest {

  private val mySequencerId = fakeSequencerId("self")
  private val otherSequencerId = fakeSequencerId("other")
  private val aMembershipWith2Nodes = Membership(mySequencerId, Set(otherSequencerId))
  private val aSequencerSnapshot = SequencerSnapshotAdditionalInfo(
    // Minimal data required for the test
    Map(mySequencerId -> PeerActiveAt(None, Some(EpochNumber(1500L)), None, None, None))
  )
}
