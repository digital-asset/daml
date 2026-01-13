// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  exerciseNode,
  fetchNode,
  lookupByKeyNode,
  templateId,
}
import com.digitalasset.canton.protocol.{ExampleContractFactory, LfGlobalKey, LfTemplateId}
import com.digitalasset.canton.util.LfTransactionBuilder.defaultPackageName
import com.digitalasset.canton.{BaseTest, LfPackageId}
import com.digitalasset.daml.lf.transaction.test.TestIdFactory
import com.digitalasset.daml.lf.transaction.test.TreeTransactionBuilder.{
  NodeOps,
  toVersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.wordspec.AnyWordSpec

class InputContractPackagesTest extends AnyWordSpec with BaseTest with TestIdFactory {

  import InputContractPackages.*

  val (cid1, cid2, cid3) = (newCid, newCid, newCid)
  val (p1, p2, p3) = (newPackageId, newPackageId, newPackageId)
  def t(pId: LfPackageId): LfTemplateId = LfTemplateId(pId, templateId.qualifiedName)

  "InputContractPackages.forTransaction" should {

    "extract package ids associated with nodes" in {

      val globalKey = LfGlobalKey.assertBuild(
        t(p2),
        Value.ValueUnit,
        defaultPackageName,
      )

      val example = toVersionedTransaction(
        exerciseNode(cid1, templateId = t(p1)).withChildren(
          lookupByKeyNode(globalKey, resolution = Some(cid2)),
          fetchNode(cid3, templateId = t(p3)),
        )
      ).transaction

      forTransaction(example) shouldBe Map(
        cid1 -> Set(p1),
        cid2 -> Set(p2),
        cid3 -> Set(p3),
      )
    }

    "return multiple package where the same contract is bound to different packages" in {

      val example = toVersionedTransaction(
        exerciseNode(cid1, templateId = t(p1)).withChildren(
          exerciseNode(cid1, templateId = t(p2)),
          exerciseNode(cid1, templateId = t(p3)),
        )
      ).transaction

      forTransaction(example) shouldBe Map(
        cid1 -> Set(p1, p2, p3)
      )
    }

  }

  "InputContractPackages.mergeToExactTuple" should {
    "work where both maps have identical keys" in {
      strictZipByKey(Map(1 -> "a", 2 -> "b"), Map(1 -> 3.0, 2 -> 4.0)) shouldBe Right(
        Map(1 -> ("a", 3.0), 2 -> ("b", 4.0))
      )
    }
    "fail where the key sets are unequal" in {
      inside(strictZipByKey(Map(1 -> "a", 2 -> "b"), Map(2 -> 4.0, 3 -> 5.0))) {
        case Left(mismatch) => mismatch shouldBe Set(1, 3)
      }
    }

  }

  "InputContractPackages.forTransactionWithContracts" should {

    val cid = newCid
    val inst = ExampleContractFactory.build()
    val tx = toVersionedTransaction(
      exerciseNode(cid, templateId = t(p1))
    ).transaction

    "combine transaction contracts with contracts instances map" in {
      forTransactionWithContracts(tx, Map(cid -> inst)) shouldBe Right(
        Map(cid -> (inst.inst, Set(p1)))
      )
    }
  }

}
