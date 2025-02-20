// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.network.data

import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer.P2PEndpointConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
  TlsP2PEndpoint,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import org.scalatest.wordspec.AsyncWordSpec

trait P2PEndpointsStoreTest extends AsyncWordSpec {
  this: AsyncWordSpec & BftSequencerBaseTest =>

  import P2PEndpointsStoreTest.*

  private[bftordering] def p2pEndpointsStore(
      createStore: () => P2PEndpointsStore[PekkoEnv]
  ): Unit =
    "P2pEndpointsStore" should {
      "create and retrieve endpoints" in {
        val store = createStore()
        for {
          updated1 <- store.addEndpoint(endpoint1)
          updated2 <- store.addEndpoint(endpoint1)
          list1 <- store.listEndpoints

          updated3 <- store.addEndpoint(endpoint3)
          list2 <- store.listEndpoints
        } yield {
          list1 should contain only endpoint1
          list2 should contain theSameElementsInOrderAs Seq(endpoint1, endpoint3)
          updated1 shouldBe true
          updated2 shouldBe false
          updated3 shouldBe true
        }
      }

      "remove endpoints" in {
        val store = createStore()
        for {
          updated1 <- store.addEndpoint(endpoint1)
          list1 <- store.listEndpoints
          updated2 <- store.removeEndpoint(endpoint1.id)
          list2 <- store.listEndpoints
          updated3 <- store.removeEndpoint(endpoint1.id)
          list3 <- store.listEndpoints
        } yield {
          list1 should contain only endpoint1
          list2 should be(empty)
          list3 should be(empty)
          updated1 shouldBe true
          updated2 shouldBe true
          updated3 shouldBe false
        }
      }

      "clear endpoints" in {
        val store = createStore()
        for {
          updated1 <- store.addEndpoint(endpoint1)
          updated2 <- store.addEndpoint(endpoint2)
          list1 <- store.listEndpoints
          _ <- store.clearAllEndpoints()
          list2 <- store.listEndpoints
          _ <- store.clearAllEndpoints()
          list3 <- store.listEndpoints
        } yield {
          list1 should contain theSameElementsInOrderAs Seq[P2PEndpoint](endpoint1, endpoint2)
          list2 should be(empty)
          list3 should be(empty)
          updated1 shouldBe true
          updated2 shouldBe true
        }
      }
    }
}

object P2PEndpointsStoreTest {

  private val endpoint1 = PlainTextP2PEndpoint("host1", Port.tryCreate(1001))
  private val endpoint2 = TlsP2PEndpoint(P2PEndpointConfig("host2", Port.tryCreate(1002)))
  private val endpoint3 = PlainTextP2PEndpoint("host3", Port.tryCreate(1003))
}
