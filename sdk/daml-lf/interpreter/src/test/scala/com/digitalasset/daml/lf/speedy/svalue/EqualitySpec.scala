// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy
package svalue

import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{Bytes, Time}
import com.digitalasset.daml.lf.interpretation.Error.ContractIdComparability
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.{Failure, Try}

class EqualitySpec extends AnyWordSpec with Inside with Matchers with ScalaCheckPropertyChecks {

  "Equality.areEquals" should {

    "fail when trying to compare local contract ID with global contract ID with same prefix" in {

      val discriminator1 = crypto.Hash.hashPrivateKey("discriminator1")
      val discriminator2 = crypto.Hash.hashPrivateKey("discriminator2")
      val suffix1 = Bytes.assertFromString("00")
      val suffix2 = Bytes.assertFromString("01")

      val cid10 = Value.ContractId.V1(discriminator1, Bytes.Empty)
      val cid11 = Value.ContractId.V1(discriminator1, suffix1)
      val cid12 = Value.ContractId.V1(discriminator1, suffix2)
      val cid21 = Value.ContractId.V1(discriminator2, suffix1)

      val local1 = Value.ContractId.V2.unsuffixed(Time.Timestamp.MinValue, discriminator1).local
      val local2 = Value.ContractId.V2.unsuffixed(Time.Timestamp.MinValue, discriminator2).local
      val cid30 = Value.ContractId.V2(local1, Bytes.Empty)
      val cid31 = Value.ContractId.V2(local1, suffix1)
      val cid32 = Value.ContractId.V2(local1, suffix2)
      val cid41 = Value.ContractId.V2(local2, suffix1)

      val List(vCid10, vCid11, vCid12, vCid21, vCid30, vCid31, vCid32, vCid41) =
        List(cid10, cid11, cid12, cid21, cid30, cid31, cid32, cid41).map(SContractId)

      val negativeTestCases =
        Table(
          "cid1" -> "cid2",
          vCid10 -> vCid10,
          vCid11 -> vCid11,
          vCid11 -> vCid12,
          vCid11 -> vCid21,
          vCid30 -> vCid30,
          vCid31 -> vCid31,
          vCid31 -> vCid32,
          vCid31 -> vCid41,
          vCid10 -> vCid30,
          vCid10 -> vCid31,
          vCid11 -> vCid30,
        )

      val positiveTestCases = Table(
        "localCid" -> "globalCid",
        vCid10 -> cid11,
        vCid10 -> cid12,
        vCid30 -> cid31,
        vCid30 -> cid32,
      )

      forEvery(negativeTestCases) { (cid1, cid2) =>
        Equality.areEqual(cid1, cid2) shouldBe (cid1 == cid2)
        Equality.areEqual(cid2, cid1) shouldBe (cid1 == cid2)
      }

      forEvery(positiveTestCases) { (localCid, globalCid) =>
        Try(Equality.areEqual(localCid, SContractId(globalCid))) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(globalCid)))
        Try(Equality.areEqual(SContractId(globalCid), localCid)) shouldBe
          Failure(SError.SErrorDamlException(ContractIdComparability(globalCid)))
      }
    }
  }
}
