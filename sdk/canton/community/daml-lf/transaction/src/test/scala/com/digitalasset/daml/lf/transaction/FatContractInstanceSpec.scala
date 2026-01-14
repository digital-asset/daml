// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, Ref, Time}
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.ContractId
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class FatContractInstanceSpec extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {

  import FatContractInstanceSpec._

  "createdAt" - {
    "now works with relative IDs" in {

      val localCreateNodeV1 = mkCreateNode(TransactionSpec.cid("local V1"))
      val localCreateNodeV2 = mkCreateNode(
        ContractId.V2.unsuffixed(Time.Timestamp.Epoch, Hash.hashPrivateKey("local V2"))
      )
      val relativeCreateNode = mkCreateNode(
        ContractId.V2.assertSuffixed(
          Time.Timestamp.Epoch,
          Hash.hashPrivateKey("relative"),
          Bytes.assertFromString("00"),
        )
      )
      val absoluteCreateNodeV1 = mkCreateNode(
        ContractId.V1(Hash.hashPrivateKey("absolute V1"), Bytes.assertFromString("80"))
      )
      val absoluteCreateNodeV2 = mkCreateNode(
        ContractId.V2.assertSuffixed(
          Time.Timestamp.Epoch,
          Hash.hashPrivateKey("absolute V2"),
          Bytes.assertFromString("80"),
        )
      )

      val invalidCases = Table(
        "create node",
        localCreateNodeV1,
        localCreateNodeV2,
        absoluteCreateNodeV1,
        absoluteCreateNodeV2,
      )
      forAll(invalidCases) { node =>
        a[IllegalArgumentException] shouldBe
          thrownBy(FatContractInstance.fromCreateNode(node, CreationTime.Now, Bytes.Empty))
      }

      FatContractInstance
        .fromCreateNode(relativeCreateNode, CreationTime.Now, Bytes.Empty)
        .createdAt shouldBe CreationTime.Now
    }
  }

  "refine types of creation time" in {
    // This test ensures that the compiler infers the right type for the createdAt field
    val fciWithAbsoluteTime =
      FatContractInstance.fromCreateNode(
        mkCreateNode(TransactionSpec.cid("cid")),
        CreationTime.CreatedAt(Time.Timestamp.Epoch),
        Bytes.Empty,
      )
    fciWithAbsoluteTime.createdAt.time shouldBe Time.Timestamp.Epoch
  }

  "Disallow global key template to to differ from contract" in {
    val node = mkCreateNode(TransactionBuilder.newV2Cid)

    val keyOpt = GlobalKeyWithMaintainers
      .build(
        templateId = Ref.Identifier.assertFromString("-dummyPkg-:NotTemplateModule:dummyName"),
        value = Value.ValueText("key1"),
        packageName = node.packageName,
        maintainers = node.signatories,
      )
      .toOption

    val createWithMismatchingKeyTemplate = node.copy(keyOpt = keyOpt)

    a[IllegalArgumentException] shouldBe
      thrownBy(
        FatContractInstance.fromCreateNode(
          createWithMismatchingKeyTemplate,
          CreationTime.CreatedAt(Time.Timestamp.now()),
          Bytes.Empty,
        )
      )
  }

}

object FatContractInstanceSpec {
  private val alice = Ref.Party.assertFromString("alice")
  private def mkCreateNode(cid: ContractId): Node.Create =
    TransactionSpec.dummyCreateNode(cid, Set(alice), Set(alice))
}
