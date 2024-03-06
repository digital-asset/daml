// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.integrations.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.data.EphemeralState
import com.digitalasset.canton.domain.sequencing.sequencer.{
  InternalSequencerPruningStatus,
  SequencerMemberStatus,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.wordspec.AnyWordSpec

class EphemeralStateTest extends AnyWordSpec with BaseTest {
  private val t1 = CantonTimestamp.Epoch.plusSeconds(1)
  private val alice = ParticipantId("alice")
  private val bob = ParticipantId("bob")
  private val carlos = ParticipantId("carlos")

  "nextCounters" should {
    "throw error if member is not registered" in {
      val state = EphemeralState(
        Map.empty,
        Map.empty,
        InternalSequencerPruningStatus(
          t1,
          Seq(SequencerMemberStatus(alice, t1, None), SequencerMemberStatus(bob, t1, None)),
        ),
      )
      an[IllegalArgumentException] should be thrownBy state.tryNextCounters(Set(carlos))
    }

    "increment existing counters and otherwise use genesis for members without an existing counter" in {
      val counters = EphemeralState(
        Map(alice -> SequencerCounter(2)),
        Map.empty,
        InternalSequencerPruningStatus(
          t1,
          Seq(SequencerMemberStatus(alice, t1, None), SequencerMemberStatus(bob, t1, None)),
        ),
      )
        .tryNextCounters(Set(alice, bob))

      counters should contain.only(
        alice -> SequencerCounter(3),
        bob -> SequencerCounter.Genesis,
      )
    }
  }
}
