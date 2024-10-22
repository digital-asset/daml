// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.snapshot

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.topology.processing.EffectiveTime
import org.scalatest.wordspec.AnyWordSpec

class SequencerSnapshotAdditionalInfoTest extends AnyWordSpec with BftSequencerBaseTest {

  "SequencerSnapshotAdditionalInfo" should {
    "successfully deserialize what we write" in {
      val snapshotAdditionalInfo = SequencerSnapshotAdditionalInfo(
        Map(
          fakeSequencerId("sequencer1") -> FirstKnownAt(None, None, None, None),
          fakeSequencerId("sequencer2") -> FirstKnownAt(
            Some(EffectiveTime(CantonTimestamp.Epoch)),
            Some(EpochNumber(7L)),
            Some(BlockNumber(70L)),
            Some(CantonTimestamp.MaxValue),
          ),
        )
      )

      val serializedSnapshotAdditionalInfo = snapshotAdditionalInfo.toProto.toByteString

      SequencerSnapshotAdditionalInfo.fromProto(
        serializedSnapshotAdditionalInfo
      ) shouldBe Right(snapshotAdditionalInfo)
    }
  }
}
