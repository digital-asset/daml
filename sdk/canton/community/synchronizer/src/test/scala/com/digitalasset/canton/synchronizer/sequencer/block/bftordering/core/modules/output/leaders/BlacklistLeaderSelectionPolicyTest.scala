// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.LeaderSelectionPolicyConfig.Blacklisting
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModuleTest.TestOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  FunctionFutureContext,
  IgnoringUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.FutureContext
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.SortedSet

class BlacklistLeaderSelectionPolicyTest extends AnyWordSpec with BaseTest {
  private val n0 = BftNodeId("node0")
  private val n1 = BftNodeId("node1")
  private val n2 = BftNodeId("node2")
  private val n3 = BftNodeId("node3")
  private val nodes = Set(n0, n1, n2, n3)

  implicit val futureContext: FutureContext[IgnoringUnitTestEnv] = new FunctionFutureContext
  private val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
  implicit val metricsContext: MetricsContext = MetricsContext.Empty

  "BlacklistLeaderSelectionPolicy" should {
    "be able to get leaders" in {
      val state = BlacklistLeaderSelectionPolicyState.FirstBlacklistLeaderSelectionPolicyState(
        testedProtocolVersion
      )
      val orderingTopology = OrderingTopology.forTesting(nodes)
      val store = createStore()
      val leaderSelectionPolicy =
        BlacklistLeaderSelectionPolicy.create(
          state,
          BftBlockOrdererConfig(),
          Blacklisting(),
          orderingTopology,
          store,
          metrics,
          loggerFactory,
        )

      leaderSelectionPolicy.getLeaders(orderingTopology, EpochNumber.First) shouldBe
        LeaderSelectionPolicy.rotateLeaders(SortedSet(n0, n1, n2, n3), EpochNumber.First)
    }

    "update state based on view information" in {
      val state = BlacklistLeaderSelectionPolicyState.FirstBlacklistLeaderSelectionPolicyState(
        testedProtocolVersion
      )
      val orderingTopology = OrderingTopology.forTesting(nodes)
      val store = createStore()
      val leaderSelectionPolicy =
        BlacklistLeaderSelectionPolicy.create(
          state,
          BftBlockOrdererConfig(),
          Blacklisting(),
          orderingTopology,
          store,
          metrics,
          loggerFactory,
        )

      leaderSelectionPolicy.addBlock(
        EpochNumber.First,
        BlockNumber.First,
        ViewNumber.First,
      ) // n0 succeeded
      leaderSelectionPolicy.addBlock(
        EpochNumber.First,
        BlockNumber(1L),
        ViewNumber(1L),
      ) // n1 failed

      leaderSelectionPolicy.saveStateFor(EpochNumber(1L), orderingTopology).run
      store.getLeaderSelectionPolicyState(EpochNumber(1L)).run shouldBe Some(
        BlacklistLeaderSelectionPolicyState.create(
          EpochNumber(1L),
          BlockNumber(16),
          Map(
            n1 -> BlacklistStatus.Blacklisted(1, 1)
          ),
        )(testedProtocolVersion)
      )

      val leaders = leaderSelectionPolicy.getLeaders(orderingTopology, EpochNumber(1L))

      leaders shouldBe
        LeaderSelectionPolicy.rotateLeaders(SortedSet(n0, n2, n3), EpochNumber(1L))
    }
  }

  private implicit class RunFakeFuture[X](future: IgnoringUnitTestEnv#FutureUnlessShutdownT[X]) {
    def run: X = future()
  }

  private def createStore(): OutputMetadataStore[IgnoringUnitTestEnv] = new TestOutputMetadataStore
}
