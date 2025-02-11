// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.{
  PeerActiveAt,
  SequencerSnapshotAdditionalInfo,
}
import org.scalatest.wordspec.AnyWordSpec

class SequencerSnapshotAdditionalInfoTest extends AnyWordSpec with BftSequencerBaseTest {

  "SequencerSnapshotAdditionalInfo" should {
    "successfully deserialize what we write" in {
      val snapshotAdditionalInfo = SequencerSnapshotAdditionalInfo(
        Map(
          fakeSequencerId("sequencer1") -> PeerActiveAt(None, None, None, None, None),
          fakeSequencerId("sequencer2") -> PeerActiveAt(
            Some(TopologyActivationTime(CantonTimestamp.Epoch)),
            Some(EpochNumber(7L)),
            Some(BlockNumber(70L)),
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
