// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.DefaultCatchupDetector
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import org.scalatest.wordspec.AnyWordSpec

import CatchupDetectorTest.*

class CatchupDetectorTest extends AnyWordSpec with BftSequencerBaseTest {

  "track the latest epoch for active nodes and determine if the node needs to switch to catch-up mode" in {
    val catchupDetector = new DefaultCatchupDetector(membership2Nodes, loggerFactory)

    catchupDetector.updateLatestKnownNodeEpoch(node1, EpochNumber.First) shouldBe true
    catchupDetector.updateLatestKnownNodeEpoch(node1, EpochNumber.First) shouldBe false

    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None

    catchupDetector.updateLatestKnownNodeEpoch(node1, EpochNumber(1)) shouldBe true
    catchupDetector.updateLatestKnownNodeEpoch(node1, EpochNumber.First) shouldBe false

    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None

    val latestKnownNodeEpoch = EpochNumber(2)
    catchupDetector.updateLatestKnownNodeEpoch(node1, latestKnownNodeEpoch) shouldBe true

    // -1 because the latest known epoch may be in progress
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe Some(
      EpochNumber(latestKnownNodeEpoch - 1)
    )

    val newMembership = Membership.forTesting(myId, otherNodes = Set.empty)
    catchupDetector.updateMembership(newMembership)

    catchupDetector.updateLatestKnownNodeEpoch(node1, latestKnownNodeEpoch) shouldBe false

    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None
  }

  "catch-up detector correctly computes f+1-highest epoch number" in {
    val catchupDetector = new DefaultCatchupDetector(membership4Nodes, loggerFactory)

    // verify that None is returned when < f+1 entries exist
    catchupDetector.updateLatestKnownNodeEpoch(node1, EpochNumber(10)) shouldBe true
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None

    // add f+1 total entries, but still not enough support to trigger catch-up
    catchupDetector.updateLatestKnownNodeEpoch(node2, EpochNumber.First) shouldBe true
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None

    // add more than f+1 entries, but still not enough future support to trigger catch-up
    catchupDetector.updateLatestKnownNodeEpoch(node3, EpochNumber.First) shouldBe true
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None

    // update entries to future epochs, but staggered to show that f+1-highest is computed
    // target should be f+1-highest (11), minus 1 == 10
    catchupDetector.updateLatestKnownNodeEpoch(node2, EpochNumber(11)) shouldBe true
    catchupDetector.updateLatestKnownNodeEpoch(node3, EpochNumber(12)) shouldBe true
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe Some(EpochNumber(10))

    // update an entry and recompute
    // target should be f+1-highest (12), minus 1 == 11
    catchupDetector.updateLatestKnownNodeEpoch(node1, EpochNumber(12)) shouldBe true
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe Some(EpochNumber(11))

    // emulate that the local node caught up to epoch 11; verify shouldCatchUp is None
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber(11)) shouldBe None
  }

  "catch-up detector correctly computes target epoch number across changing memberships" in {
    val catchupDetector = new DefaultCatchupDetector(membership4Nodes, loggerFactory)

    // start with N=4 and exactly f+1 votes
    catchupDetector.updateLatestKnownNodeEpoch(node1, EpochNumber(10)) shouldBe true
    catchupDetector.updateLatestKnownNodeEpoch(node2, EpochNumber(10)) shouldBe true
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe Some(EpochNumber(9))

    // switch from N=4 to N=7, which changes f and means we are short one vote
    catchupDetector.updateMembership(membership7Nodes)
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None
    // add one more vote from valid peer, which now detects catch-up as necessary
    catchupDetector.updateLatestKnownNodeEpoch(node3, EpochNumber(10)) shouldBe true
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe Some(EpochNumber(9))

    // switch back to N=4, but to an alternate topology missing two of the original peers
    // this causes two of the original votes to be discarded, and shouldCatchUp is None
    val altMembership4Nodes = Membership.forTesting(myId, Set(node3, node4, node5))
    catchupDetector.updateMembership(altMembership4Nodes)
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe None
    // one more vote from an active peer reaches the f+1 threshold again
    catchupDetector.updateLatestKnownNodeEpoch(node4, EpochNumber(10)) shouldBe true
    catchupDetector.shouldCatchUpTo(localEpoch = EpochNumber.First) shouldBe Some(EpochNumber(9))
  }
}

object CatchupDetectorTest {

  private val myId = BftNodeId("self")
  private val node1 = BftNodeId("node1")
  private val node2 = BftNodeId("node2")
  private val node3 = BftNodeId("node3")
  private val node4 = BftNodeId("node4")
  private val node5 = BftNodeId("node5")
  private val node6 = BftNodeId("node6")
  private val membership2Nodes = Membership.forTesting(myId, Set(node1))
  private val membership4Nodes = Membership.forTesting(myId, Set(node1, node2, node3))
  private val membership7Nodes =
    Membership.forTesting(myId, Set(node1, node2, node3, node4, node5, node6, node6))
}
