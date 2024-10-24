// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import data.Bytes

import Value._
import test.ValueGenerators.coidGen
import org.scalacheck.Arbitrary
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec
import org.scalatestplus.scalacheck.{Checkers, ScalaCheckPropertyChecks}
import scalaz.scalacheck.{ScalazProperties => SzP}

class ValueSpec
    extends AnyFreeSpec
    with Matchers
    with Inside
    with Checkers
    with ScalaCheckPropertyChecks {
  "ContractID.V1.build" - {

    "rejects too long suffix" in {

      def suffix(size: Int) =
        Bytes.fromByteArray(Array.iterate(0.toByte, size)(b => (b + 1).toByte))

      val hash = crypto.Hash.hashPrivateKey("some hash")
      import ContractId.V1.build
      build(hash, suffix(0)) shouldBe Symbol("right")
      build(hash, suffix(94)) shouldBe Symbol("right")
      build(hash, suffix(95)) shouldBe Symbol("left")
      build(hash, suffix(96)) shouldBe Symbol("left")
      build(hash, suffix(127)) shouldBe Symbol("left")

    }

    "finds cid in contract id value" in {
      val hash = crypto.Hash.hashPrivateKey("some other hash")
      val cid = ContractId.V1(hash)
      val value = Value.ValueContractId(cid)

      value.cids shouldBe Set(cid)
    }

  }

  "ContractId" - {
    type T = ContractId
    implicit val arbT: Arbitrary[T] = Arbitrary(coidGen)
    "Order" - {
      "obeys Order laws" in checkLaws(SzP.order.laws[T])
    }
  }

  // XXX can factor like FlatSpecCheckLaws
  private def checkLaws(props: org.scalacheck.Properties) =
    forEvery(Table(("law", "property"), props.properties.toSeq: _*)) { (_, p) =>
      check(p, minSuccessful(20))
    }
}
