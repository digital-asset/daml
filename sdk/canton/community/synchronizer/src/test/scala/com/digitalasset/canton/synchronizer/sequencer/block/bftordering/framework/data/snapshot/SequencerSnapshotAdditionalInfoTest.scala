// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
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
            epochNumber = None,
            firstBlockNumberInEpoch = None,
            epochTopologyQueryTimestamp = None,
            epochCouldAlterOrderingTopology = None,
            previousBftTime = None,
          ),
          BftNodeId("sequencer2") -> NodeActiveAt(
            aTopologyActivationTime,
            Some(EpochNumber(7L)),
            Some(BlockNumber(70L)),
            Some(aTopologyActivationTime),
            Some(true),
            Some(CantonTimestamp.MinValue),
          ),
        )
      )

      val serializedSnapshotAdditionalInfo = snapshotAdditionalInfo.toProto30.toByteString

      SequencerSnapshotAdditionalInfo.fromProto(
        serializedSnapshotAdditionalInfo
      ) shouldBe Right(snapshotAdditionalInfo)
    }
  }
}
