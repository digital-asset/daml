// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package value

import com.daml.lf.EitherAssertions
import com.daml.lf.data._
import com.daml.lf.transaction.{TransactionVersion, Versioned}
import com.daml.lf.value.{ValueOuterClass => proto}
import org.scalacheck.{Arbitrary, Shrink}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ValueCoderSpec
    extends AnyWordSpec
    with Matchers
    with EitherAssertions
    with ScalaCheckPropertyChecks {

  import Value._
  import test.ValueGenerators._

  implicit val noStringShrink: Shrink[String] = Shrink.shrinkAny[String]

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  "encode" should {
    "fail gracefully when serialized message exceeding 2GB" in {

      val ver = TransactionVersion.StableVersions.max

      val value0 = ValueText("a" * (1024 * 1024 * 1024))
      val value1 = ValueList(FrontStack(value0, value0))

      ValueCoder.encodeValue(ValueCoder.CidEncoder, ver, value0) shouldBe a[Right[_, _]]
      ValueCoder.encodeValue(ValueCoder.CidEncoder, ver, value1) shouldBe a[Left[_, _]]
    }
  }

  "encode-decode" should {
    "do Int" in {
      forAll(Arbitrary.arbLong.arbitrary, transactionVersionGen())((i, v) =>
        testRoundTrip(ValueInt64(i), v)
      )
    }

    "do Bool" in {
      forAll(Arbitrary.arbBool.arbitrary, transactionVersionGen())((b, v) =>
        testRoundTrip(ValueBool(b), v)
      )
    }

    "do Numeric" in {
      import test.ValueGenerators.Implicits._

      forAll("Numeric scale", "Decimal (BigDecimal) invariant") {
        (s: Numeric.Scale, d: BigDecimal) =>
          // we are filtering on decimals invariant under string conversion
          whenever(Numeric.fromBigDecimal(s, d).isRight) {
            val Right(dec) = Numeric.fromBigDecimal(s, d)
            val value = ValueNumeric(dec)
            val recoveredDecimal = ValueCoder.decodeValue(
              ValueCoder.CidDecoder,
              TransactionVersion.minVersion,
              assertRight(
                ValueCoder
                  .encodeValue(
                    ValueCoder.CidEncoder,
                    TransactionVersion.minVersion,
                    value,
                  )
              ),
            ) match {
              case Right(ValueNumeric(x)) => x
              case x => fail(s"should have got a numeric back, got $x")
            }
            Numeric.toUnscaledString(value.value) shouldEqual Numeric.toUnscaledString(
              recoveredDecimal
            )
          }
      }
    }

    "do Text" in {
      forAll("Text (String) invariant", transactionVersionGen())((t, v) =>
        testRoundTrip(ValueText(t), v)
      )
    }

    "do Party" in {
      forAll(party, transactionVersionGen())((p, v) => testRoundTrip(ValueParty(p), v))
    }

    "do TimeStamp" in {
      forAll(timestampGen, transactionVersionGen())((t, v) => testRoundTrip(ValueTimestamp(t), v))
    }

    "do Date" in {
      forAll(dateGen, transactionVersionGen())((d, v) => testRoundTrip(ValueDate(d), v))
    }

    "do ContractId" in {
      forAll(coidValueGen, transactionVersionGen())(testRoundTrip)
    }

    "do ContractId V0 in any ValueVersion" in forAll(coidValueGen, transactionVersionGen())(
      testRoundTrip
    )

    "do lists" in {
      forAll(valueListGen, transactionVersionGen())(testRoundTrip)
    }

    "do optionals" in {
      forAll(valueOptionalGen, transactionVersionGen())(testRoundTrip)
    }

    "do maps" in {
      forAll(valueMapGen, transactionVersionGen())(testRoundTrip)
    }

    "do genMaps" in {
      forAll(valueGenMapGen, transactionVersionGen())(
        testRoundTrip
      )
    }

    "do variant" in {
      forAll(variantGen, transactionVersionGen())(testRoundTrip)
    }

    "do record" in {
      forAll(recordGen, transactionVersionGen())(testRoundTrip)
    }

    "do unit" in {
      forAll(transactionVersionGen())(testRoundTrip(ValueUnit, _))
    }

    "do identifier" in {
      forAll(idGen) { i =>
        ValueCoder.decodeIdentifier(ValueCoder.encodeIdentifier(i)) shouldEqual Right(i)
      }
    }

    "do identifier with supported override version" in forAll(idGen, transactionVersionGen()) {
      (i, _) =>
        val ei = ValueCoder.encodeIdentifier(i)
        ValueCoder.decodeIdentifier(ei) shouldEqual Right(i)
    }

    "do versioned value with supported override version" in forAll(versionedValueGen) {
      case Versioned(version, value) => testRoundTrip(value, version)
    }

  }

  "decode" should {
    "do deep record" in {
      def toNat(
          i: Int,
          acc: ValueRecord = ValueRecord(None, ImmArray.Empty),
      ): ValueRecord =
        if (i <= 0) acc
        else toNat(i - 1, ValueRecord(None, ImmArray(None -> acc)))

      val n = toNat(100)

      // We double check that 100 is the maximum
      ValueCoder
        .encodeValue(
          ValueCoder.CidEncoder,
          TransactionVersion.maxVersion,
          toNat(1, n), // 101
        ) shouldBe a[Left[_, _]]

      val encoded = assertRight(
        ValueCoder
          .encodeValue(ValueCoder.CidEncoder, TransactionVersion.maxVersion, n)
      )

      ValueCoder.decodeValue(
        ValueCoder.CidDecoder,
        TransactionVersion.maxVersion,
        encoded,
      ) shouldBe Right(n)
    }
  }

  def testRoundTrip(value0: Value, version: TransactionVersion): Assertion = {
    val normalizedValue = transaction.Util.assertNormalizeValue(value0, version)
    val encoded: proto.VersionedValue = assertRight(
      ValueCoder
        .encodeVersionedValue(ValueCoder.CidEncoder, Versioned(version, value0))
    )
    val decoded: VersionedValue = assertRight(
      ValueCoder.decodeVersionedValue(ValueCoder.CidDecoder, encoded)
    )

    decoded shouldEqual Versioned(version, normalizedValue)

    // emulate passing encoded proto message over wire

    val encodedSentOverWire: proto.VersionedValue =
      proto.VersionedValue.parseFrom(encoded.toByteArray)
    val decodedSentOverWire: VersionedValue = assertRight(
      ValueCoder.decodeVersionedValue(ValueCoder.CidDecoder, encodedSentOverWire)
    )

    decodedSentOverWire shouldEqual Versioned(version, normalizedValue)
  }
}
