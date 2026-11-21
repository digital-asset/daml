// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.DefaultHowLongToBlackList
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.LeaderSelectionPolicyConfig.Blacklisting
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.LeaderSelectionPolicyConfig.HowManyCanWeBlacklist.NoBlacklisting
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import org.scalatest.wordspec.AnyWordSpec

class BlacklistLeaderSelectionPolicyStateTest extends AnyWordSpec with BaseTest {

  private def n(i: Int): BftNodeId = BftNodeId(s"node$i")
  private val n0 = n(0)
  private val n1 = n(1)
  private val n2 = n(2)
  private val n3 = n(3)
  private val orderingTopology = OrderingTopology.forTesting(Set(n0, n1, n2, n3))

  private val epochLength = EpochLength(10)

  private val blockToLeaderAll: Map[BlockNumber, BftNodeId] = Map(
    BlockNumber(0L) -> n0,
    BlockNumber(1L) -> n1,
    BlockNumber(2L) -> n2,
    BlockNumber(3L) -> n3,
  )

  private val blockToLeaderAllWithoutN0 = blockToLeaderAll.removed(BlockNumber(0L))

  private val config = Blacklisting()

  private def initState(
      blacklist: (BftNodeId, BlacklistStatus.BlacklistStatusMark)*
  ): BlacklistLeaderSelectionPolicyState = BlacklistLeaderSelectionPolicyState.create(
    EpochNumber.First,
    BlockNumber.First,
    Map.from(blacklist),
  )(testedProtocolVersion)

  private def stateNextEpoch(
      blacklist: (BftNodeId, BlacklistStatus.BlacklistStatusMark)*
  ): BlacklistLeaderSelectionPolicyState = BlacklistLeaderSelectionPolicyState.create(
    EpochNumber(1L),
    BlockNumber(10L),
    Map.from(blacklist),
  )(testedProtocolVersion)

  "BlacklistLeaderSelectionPolicyState" should {
    "a clean node" should {
      "stay clean if not punished" in {
        initState().update(
          orderingTopology,
          config,
          epochLength,
          blockToLeaderAll,
          Set.empty,
        ) shouldBe stateNextEpoch()
      }

      "be blacklisted if punished" in {
        initState().update(
          orderingTopology,
          config,
          epochLength,
          blockToLeaderAll,
          Set(n0),
        ) shouldBe stateNextEpoch(n0 -> BlacklistStatus.Blacklisted(1, 1))
      }
    }

    "a blacklisted node" should {
      "stay blacklisted if still time" in {
        initState(
          n0 -> BlacklistStatus.Blacklisted(1, 1)
        ).update(
          orderingTopology,
          config,
          epochLength,
          blockToLeaderAllWithoutN0,
          Set.empty,
        ) shouldBe stateNextEpoch(n0 -> BlacklistStatus.Blacklisted(1, 0))
      }

      "go on trial if waited long enough" in {
        initState(
          n0 -> BlacklistStatus.Blacklisted(1, 0)
        ).update(
          orderingTopology,
          config,
          epochLength,
          blockToLeaderAllWithoutN0,
          Set.empty,
        ) shouldBe stateNextEpoch(n0 -> BlacklistStatus.OnTrial(1))
      }
    }

    "a node on trial" should {
      "become clean if succeed" in {
        initState(
          n0 -> BlacklistStatus.OnTrial(1)
        ).update(
          orderingTopology,
          config,
          epochLength,
          blockToLeaderAll,
          Set.empty,
        ) shouldBe stateNextEpoch()
      }

      "become blacklisted if punished" in {
        initState(
          n0 -> BlacklistStatus.OnTrial(1)
        ).update(
          orderingTopology,
          config,
          epochLength,
          blockToLeaderAll,
          Set(n0),
        ) shouldBe stateNextEpoch(n0 -> BlacklistStatus.Blacklisted(2, 2))
      }

      "stay on trial if did not participate" in {
        initState(
          n0 -> BlacklistStatus.OnTrial(1)
        ).update(
          orderingTopology,
          config,
          epochLength,
          blockToLeaderAllWithoutN0,
          Set.empty,
        ) shouldBe stateNextEpoch(n0 -> BlacklistStatus.OnTrial(1))
      }
    }

    "should only select clean and nodes on trial" in {
      initState(n1 -> BlacklistStatus.OnTrial(1), n2 -> BlacklistStatus.Blacklisted(1, 1))
        .selectLeaders(orderingTopology, config) shouldBe Set(n0, n1, n3)
    }

    "should only drop up to f nodes" in {
      initState(n1 -> BlacklistStatus.Blacklisted(2, 2), n2 -> BlacklistStatus.Blacklisted(1, 1))
        .selectLeaders(orderingTopology, config) shouldBe Set(n0, n2, n3)
    }

    "should not drop any if config says so" in {
      initState(n1 -> BlacklistStatus.Blacklisted(2, 1), n2 -> BlacklistStatus.Blacklisted(1, 1))
        .selectLeaders(
          orderingTopology,
          Blacklisting(
            howLongToBlackList = DefaultHowLongToBlackList,
            howManyCanWeBlacklist = NoBlacklisting,
          ),
        ) shouldBe Set(n0, n1, n2, n3)
    }
  }
}
