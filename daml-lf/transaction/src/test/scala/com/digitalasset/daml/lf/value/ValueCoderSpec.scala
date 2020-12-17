// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.value

import com.daml.lf.EitherAssertions
import com.daml.lf.data.Ref.Party
import com.daml.lf.data._
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value._
import com.daml.lf.value.{ValueOuterClass => proto}
import org.scalacheck.Shrink
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValueCoderSpec
    extends AnyWordSpec
    with Matchers
    with EitherAssertions
    with ScalaCheckPropertyChecks {

  import test.ValueGenerators._

  implicit val noStringShrink: Shrink[String] = Shrink.shrinkAny[String]

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  "encode-decode" should {
    "do Int" in {
      forAll("Int64 (Long) invariant") { i: Long =>
        val value = ValueInt64(i)
        testRoundTrip(TransactionVersion.minVersion, value)
      }
    }

    "do Bool" in {
      forAll("Bool invariant") { b: Boolean =>
        val value = ValueBool(b)
        testRoundTrip(TransactionVersion.minVersion, value)
      }
    }

    "do Numeric" in {
      import test.ValueGenerators.Implicits._

      forAll("Numeric scale", "Decimal (BigDecimal) invariant") {
        (s: Numeric.Scale, d: BigDecimal) =>
          // we are filtering on decimals invariant under string conversion
          whenever(Numeric.fromBigDecimal(s, d).isRight) {
            val Right(dec) = Numeric.fromBigDecimal(s, d)
            val value = ValueNumeric(dec)
            val recoveredDecimal = ValueCoder.decodeValue[ContractId](
              ValueCoder.CidDecoder,
              TransactionVersion.minVersion,
              assertRight(
                ValueCoder
                  .encodeValue[ContractId](
                    ValueCoder.CidEncoder,
                    TransactionVersion.minVersion,
                    value),
              ),
            ) match {
              case Right(ValueNumeric(x)) => x
              case x => fail(s"should have got a numeric back, got $x")
            }
            Numeric.toUnscaledString(value.value) shouldEqual Numeric.toUnscaledString(
              recoveredDecimal,
            )
          }
      }
    }

    "do Text" in {
      forAll("Text (String) invariant") { t: String =>
        val value = ValueText(t)
        testRoundTrip(TransactionVersion.minVersion, value)
      }
    }

    "do Party" in {
      forAll(party) { p: Party =>
        val value = ValueParty(p)
        testRoundTrip(TransactionVersion.minVersion, value)
      }
    }

    "do TimeStamp" in {
      forAll(timestampGen) { t: Time.Timestamp => // TODO: fails with Longs
        testRoundTrip(TransactionVersion.minVersion, ValueTimestamp(t))
      }
    }

    "do Date" in {
      forAll(dateGen) { d: Time.Date =>
        testRoundTrip(TransactionVersion.minVersion, ValueDate(d))
      }
    }

    "do ContractId" in {
      forAll(coidValueGen) { v: Value[ContractId] =>
        testRoundTrip(TransactionVersion.minVersion, v)
      }
    }

    "do ContractId V0 in any ValueVersion" in forAll(coidValueGen, transactionVersionGen())(
      testRoundTripWithVersion
    )

    "do lists" in {
      forAll(valueListGen) { v: ValueList[ContractId] =>
        testRoundTrip(TransactionVersion.minVersion, v)
      }
    }

    "do optionals" in {
      forAll(valueOptionalGen) { v: ValueOptional[ContractId] =>
        testRoundTrip(TransactionVersion.minVersion, v)
      }
    }

    "do maps" in {
      forAll(valueMapGen) { v: ValueTextMap[ContractId] =>
        testRoundTrip(TransactionVersion.minVersion, v)
      }
    }

    "do genMaps" in {
      forAll(valueGenMapGen) { v: ValueGenMap[ContractId] =>
        testRoundTrip(TransactionVersion.minGenMap, v)
      }
    }

    "do variant" in {
      forAll(variantGen) { v: ValueVariant[ContractId] =>
        testRoundTrip(TransactionVersion.minVersion, v)
      }
    }

    "do record" in {
      forAll(recordGen) { v: ValueRecord[ContractId] =>
        testRoundTrip(TransactionVersion.minVersion, v)
      }
    }

    "do unit" in {
      testRoundTrip(TransactionVersion.minVersion, ValueUnit)
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
      case VersionedValue(version, value) => testRoundTripWithVersion(value, version)
    }
  }

  def testRoundTrip(version: TransactionVersion, value: Value[ContractId]): Assertion = {
    val recovered = ValueCoder.decodeValue(
      ValueCoder.CidDecoder,
      version,
      assertRight(ValueCoder.encodeValue[ContractId](ValueCoder.CidEncoder, version, value)),
    )
    val bytes =
      assertRight(
        ValueCoder.encodeVersionedValue(
          ValueCoder.CidEncoder,
          VersionedValue(version, value),
        )
      ).toByteArray

    val fromToBytes = ValueCoder.valueFromBytes(
      ValueCoder.CidDecoder,
      bytes,
    )
    Right(value) shouldEqual recovered
    Right(value) shouldEqual fromToBytes
  }

  def testRoundTripWithVersion(
      value0: Value[ContractId],
      version: TransactionVersion): Assertion = {
    val encoded: proto.VersionedValue = assertRight(
      ValueCoder
        .encodeVersionedValue(ValueCoder.CidEncoder, VersionedValue(version, value0)),
    )
    val decoded: VersionedValue[ContractId] = assertRight(
      ValueCoder.decodeVersionedValue(ValueCoder.CidDecoder, encoded),
    )

    decoded.value shouldEqual value0
    decoded.version shouldEqual version

    // emulate passing encoded proto message over wire

    val encodedSentOverWire: proto.VersionedValue =
      proto.VersionedValue.parseFrom(encoded.toByteArray)
    val decodedSentOverWire: VersionedValue[ContractId] = assertRight(
      ValueCoder.decodeVersionedValue(ValueCoder.CidDecoder, encodedSentOverWire),
    )

    decodedSentOverWire.value shouldEqual value0
    decodedSentOverWire.version shouldEqual version
  }
}
