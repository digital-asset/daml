// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit

import com.digitalasset.canton.crypto.{Fingerprint, Signature}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.FingerprintKeyId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import org.scalatest.wordspec.AnyWordSpec

class OrderingTopologyTest extends AnyWordSpec with BftSequencerBaseTest {
  import OrderingTopologyTest.*

  "The ordering topology" should {
    "consider a node and key authorized" when {
      "both the node and the key are in the topology" in {

        val topology = OrderingTopology.forTesting(Set(aNode))
        topology.isAuthorized(
          aNode,
          FingerprintKeyId.toBftKeyId(Signature.noSignature.signedBy),
        ) shouldBe true
      }
    }
  }

  "The ordering topology" should {
    "consider a node and key unauthorized" when {

      "the node is not in the topology" in {

        val topology = OrderingTopology.forTesting(Set())
        topology.isAuthorized(
          aNode,
          FingerprintKeyId.toBftKeyId(Signature.noSignature.signedBy),
        ) shouldBe false
      }

      "the node is in the topology but the key is not" in {

        val topology = OrderingTopology.forTesting(Set(aNode))
        topology.isAuthorized(
          aNode,
          FingerprintKeyId.toBftKeyId(Fingerprint.tryFromString("not-in-topology")),
        ) shouldBe false
      }
    }
  }
}
object OrderingTopologyTest {
  private val aNode = BftNodeId("ANode")
}
