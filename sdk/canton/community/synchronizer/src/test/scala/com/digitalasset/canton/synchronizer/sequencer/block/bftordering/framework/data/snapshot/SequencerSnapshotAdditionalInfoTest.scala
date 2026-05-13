// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.{
  BlacklistLeaderSelectionPolicyState,
  BlacklistStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import org.scalatest.wordspec.AnyWordSpec

class SequencerSnapshotAdditionalInfoTest extends AnyWordSpec with BftSequencerBaseTest {

  "SequencerSnapshotAdditionalInfo" should {
    "successfully deserialize what we write" in {
      val aTopologyActivationTime = TopologyActivationTime(CantonTimestamp.Epoch)
      val snapshotAdditionalInfo = SequencerSnapshotAdditionalInfo(
        Map(
          BftNodeId("sequencer1") -> NodeActiveAt(
            aTopologyActivationTime,
            startEpochNumber = None,
            firstBlockNumberInStartEpoch = None,
            startEpochTopologyQueryTimestamp = None,
            startEpochCouldAlterOrderingTopology = None,
            previousBftTime = None,
            previousEpochTopologyQueryTimestamp = None,
            leaderSelectionPolicyState = None,
          ),
          BftNodeId("sequencer2") -> NodeActiveAt(
            aTopologyActivationTime,
            Some(EpochNumber(7L)),
            Some(BlockNumber(70L)),
            Some(aTopologyActivationTime),
            Some(true),
            Some(CantonTimestamp.MinValue),
            Some(TopologyActivationTime(aTopologyActivationTime.value.minusSeconds(1L))),
            Some(
              BlacklistLeaderSelectionPolicyState.create(
                EpochNumber(7L),
                BlockNumber(70L),
                Map[BftNodeId, BlacklistStatus.BlacklistStatusMark](
                  BftNodeId("node1") -> BlacklistStatus.OnTrial(1L),
                  BftNodeId("node2") -> BlacklistStatus.Blacklisted(1L, 2L),
                ),
              )(
                testedProtocolVersion
              )
            ),
          ),
        )
      )

      val serializedSnapshotAdditionalInfo = snapshotAdditionalInfo.toProto30.toByteString

      SequencerSnapshotAdditionalInfo.fromProto(
        testedProtocolVersion,
        serializedSnapshotAdditionalInfo,
      ) shouldBe Right(snapshotAdditionalInfo)
    }
  }
}
