// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.topology.DefaultTestIdentities.domainManager
import com.digitalasset.canton.topology.{DomainId, TestingOwnerWithKeys, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

class TopologyChangeOpTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  import TopologyChangeOp.*

  private lazy val uid = UniqueIdentifier.tryFromProtoPrimitive("da::tluafed")

  private lazy val factory: TestingOwnerWithKeys =
    new TestingOwnerWithKeys(domainManager, loggerFactory, parallelExecutionContext)

  private lazy val addSignedTx: SignedTopologyTransaction[Add] =
    factory.mkAdd(IdentifierDelegation(uid, factory.SigningKeys.key1))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private lazy val removeSignedTx: SignedTopologyTransaction[Remove] = {
    val reversedTx = addSignedTx.transaction.reverse

    addSignedTx
      .update(transaction = reversedTx)
      .asInstanceOf[SignedTopologyTransaction[Remove]]
  }

  private lazy val replaceSignedTx: SignedTopologyTransaction[Replace] = factory.mkDmGov(
    DomainParametersChange(
      DomainId(uid),
      TestDomainParameters.defaultDynamic,
    ),
    factory.SigningKeys.key1,
  )

  "TopologyChangeOp" should {
    "detect Add type" in {
      topologyAddChecker.isOfType(Add) shouldBe true
      topologyAddChecker.isOfType(Remove) shouldBe false
      topologyAddChecker.isOfType(Replace) shouldBe false
    }

    "detect Remove type" in {
      topologyRemoveChecker.isOfType(Add) shouldBe false
      topologyRemoveChecker.isOfType(Remove) shouldBe true
      topologyRemoveChecker.isOfType(Replace) shouldBe false
    }

    "detect Replace type" in {
      topologyReplaceChecker.isOfType(Add) shouldBe false
      topologyReplaceChecker.isOfType(Remove) shouldBe false
      topologyReplaceChecker.isOfType(Replace) shouldBe true
    }

    "detect Positive type" in {
      topologyPositiveChecker.isOfType(Add) shouldBe true
      topologyPositiveChecker.isOfType(Remove) shouldBe false
      topologyPositiveChecker.isOfType(Replace) shouldBe true
    }
  }

  "TopologyChangeOp.select" should {
    "detect Add type" in {
      TopologyChangeOp.select[Add](addSignedTx) shouldBe Some(addSignedTx)
      TopologyChangeOp.select[Add](removeSignedTx) shouldBe None
      TopologyChangeOp.select[Add](replaceSignedTx) shouldBe None
    }

    "detect Remove type" in {
      TopologyChangeOp.select[Remove](addSignedTx) shouldBe None
      TopologyChangeOp.select[Remove](removeSignedTx) shouldBe Some(removeSignedTx)
      TopologyChangeOp.select[Remove](replaceSignedTx) shouldBe None
    }

    "detect Replace type" in {
      TopologyChangeOp.select[Replace](addSignedTx) shouldBe None
      TopologyChangeOp.select[Replace](removeSignedTx) shouldBe None
      TopologyChangeOp.select[Replace](replaceSignedTx) shouldBe Some(replaceSignedTx)
    }
  }
}
