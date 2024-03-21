// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.codegen

import com.daml.sample.MyMain.NameClashRecordVariant
import NameClashRecordVariant.{NameClashRecordVariantA, NameClashRecordVariantB}
import com.daml.ledger.client.binding.{Primitive => P, Value}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class NameClashRecordVariantUT
    extends AnyWordSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

  "generated variants have compatible read and write methods" in forAll(nameClashRecordVariantGen) {
    a1 =>
      val b = Value.encode(a1)
      val a2 = Value.decode[NameClashRecordVariant](b)
      Some(a1) shouldBe a2
  }

  def nameClashRecordVariantGen: Gen[NameClashRecordVariant] =
    Gen.oneOf(nameClashRecordVariantAGen, nameClashRecordVariantBGen)

  def nameClashRecordVariantAGen: Gen[NameClashRecordVariantA] =
    for {
      x <- arbitrary[P.Int64]
      y <- arbitrary[P.Int64]
      z <- arbitrary[P.Int64]
    } yield NameClashRecordVariantA(x, y, z)

  def nameClashRecordVariantBGen: Gen[NameClashRecordVariantB] =
    for {
      x <- arbitrary[P.Int64]
      y <- arbitrary[P.Int64]
      z <- arbitrary[P.Int64]
    } yield NameClashRecordVariantB(x, y, z)
}
