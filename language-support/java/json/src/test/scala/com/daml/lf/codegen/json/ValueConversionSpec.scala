// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.json

import com.daml.ledger.javaapi.{data => JData}
import com.daml.lf.data._
import com.daml.lf.value.{Value => LfValue}
import com.daml.lf.value.test.ValueGenerators
import org.scalacheck.{Arbitrary, Shrink}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ValueConversionSpec extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {
  import LfValue._
  import ValueGenerators._

  implicit val noStringShrink: Shrink[String] = Shrink.shrinkAny[String]

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  "value conversion" should {
//    "do values" in {
//      "do values" in forAll(valueGen)(testRoundTrip)
//    }
    "do Int" in {
      forAll(Arbitrary.arbLong.arbitrary)(i => testRoundTrip(ValueInt64(i)))
    }

    "do Bool" in {
      forAll(Arbitrary.arbBool.arbitrary)(b => testRoundTrip(ValueBool(b)))
    }

    "do Numeric" in {
      import ValueGenerators.Implicits._

      forAll("Numeric scale", "Decimal (BigDecimal) invariant") {
        (s: Numeric.Scale, d: BigDecimal) =>
          // we are filtering on decimals invariant under string conversion
          whenever(Numeric.fromBigDecimal(s, d).isRight) {
            val Right(dec) = Numeric.fromBigDecimal(s, d)
            val value = ValueNumeric(dec)
            val recoveredDecimal =
              ValueConversion.toLfValue(ValueConversion.fromLfValue(value)) match {
                case ValueNumeric(x) => x
                case x => fail(s"should have got a numeric back, got $x")
              }
            Numeric.toUnscaledString(value.value) shouldEqual Numeric.toUnscaledString(
              recoveredDecimal
            )
          }
      }
    }

    "do Text" in {
      forAll("Text (String) invariant")((t: String) => testRoundTrip(ValueText(t)))
    }

    "do Party" in {
      forAll(party)(p => testRoundTrip(ValueParty(p)))
    }

    "do TimeStamp" in {
      forAll(timestampGen)(t => testRoundTrip(ValueTimestamp(t)))
    }

    "do Date" in {
      forAll(dateGen)(d => testRoundTrip(ValueDate(d)))
    }

    "do ContractId" in {
      forAll(coidValueGen)(testRoundTrip)
    }

    "do ContractId V0 in any ValueVersion" in {
      forAll(coidValueGen)(testRoundTrip)
    }

    "do lists" in {
      forAll(valueListGen)(testRoundTrip)
    }

    "do optionals" in {
      forAll(valueOptionalGen)(testRoundTrip)
    }

    "do maps" in {
      forAll(valueMapGen)(testRoundTrip)
    }

    "do genMaps" in {
      forAll(valueGenUniqueKeysMapGen) { lfValue =>
        val jValue: JData.Value = ValueConversion.fromLfValue(lfValue)
        val convertedLfGenMap: LfValue = ValueConversion.toLfValue(jValue)

        val lfGenEntrySet = extractMapEntries(lfValue)
        val convertedLfGenEntrySet = extractMapEntries(convertedLfGenMap)

        convertedLfGenEntrySet shouldEqual lfGenEntrySet
      }
    }

    "do variant" in {
      forAll(variantGen)(testRoundTrip)
    }

    "do record" in {
      forAll(recordGen)(testRoundTrip)
    }

    "do unit" in {
      testRoundTrip(ValueUnit)
    }
  }

  private def testRoundTrip(lfValue: LfValue): Assertion = {
    val jValue: JData.Value = ValueConversion.fromLfValue(lfValue)
    val convertedLfValue: LfValue = ValueConversion.toLfValue(jValue)

    convertedLfValue shouldEqual lfValue
  }

  private def extractMapEntries(valueGenMap: LfValue): Set[(LfValue, LfValue)] = {
    valueGenMap match {
      case LfValue.ValueGenMap(entries) => entries.toList.toSet
      case x => fail(s"should have got a value gen map back, got $x")
    }
  }
}
