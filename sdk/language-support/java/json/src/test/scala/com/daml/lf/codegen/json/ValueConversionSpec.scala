// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.json

import com.daml.ledger.javaapi.{data => JData}
import com.daml.lf.value.{Value => LfValue}
import com.daml.lf.value.test.ValueGenerators
import org.scalacheck.{Gen, Shrink}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ValueConversionSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {
  import ValueGenerators._

  implicit val noStringShrink: Shrink[String] = Shrink.shrinkAny[String]

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  "value conversion" should {
    "do values" in {
      def nested: Gen[LfValue] =
        Gen.oneOf(
          valueListGen,
          variantGen,
          recordGen,
          valueOptionalGen,
          valueTextMapGen,
          valueGenMapGen,
        )
      forAll(valueGen(nested))(testRoundTrip)
    }
  }

  private def testRoundTrip(lfValue: LfValue): Assertion = {
    val jValue: JData.Value = ValueConversion.fromLfValue(lfValue)
    val convertedLfValue: LfValue = ValueConversion.toLfValue(jValue)

    convertedLfValue shouldEqual lfValue
  }
}
