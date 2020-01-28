// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.value

import com.digitalasset.daml.lf.EitherAssertions
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.{ValueOuterClass => proto}
import org.scalacheck.Shrink
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Assertion, Matchers, WordSpec}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ValueCoderSpec extends WordSpec with Matchers with EitherAssertions with PropertyChecks {

  import ValueGenerators._

  implicit val noStringShrink: Shrink[String] = Shrink.shrinkAny[String]

  private[this] val lastDecimalVersion = ValueVersion("5")

  private[this] val firstNumericVersion = ValueVersion("6")

  private[this] val defaultValueVersion = ValueVersions.acceptedVersions.lastOption getOrElse sys
    .error("there are no allowed versions! impossible! but could it be?")

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  "encode-decode" should {
    "do Int" in {
      forAll("Int64 (Long) invariant") { i: Long =>
        val value = ValueInt64(i)
        testRoundTrip(value)
      }
    }

    "do Bool" in {
      forAll("Bool invariant") { b: Boolean =>
        val value = ValueBool(b)
        testRoundTrip(value)
      }
    }

    "do Decimal" in {
      forAll("Decimal (BigDecimal) invariant") { d: BigDecimal =>
        // we are filtering on decimals invariant under string conversion
        whenever(Decimal.fromBigDecimal(d).isRight) {
          val Right(dec) = Decimal.fromBigDecimal(d)
          val value = ValueNumeric(dec)
          val recoveredDecimal = ValueCoder.decodeValue[ContractId](
            defaultCidDecode,
            lastDecimalVersion,
            assertRight(
              ValueCoder
                .encodeValue[ContractId](defaultCidEncode, lastDecimalVersion, value),
            ),
          ) match {
            case Right(ValueNumeric(d)) => d
            case x => fail(s"should have got a decimal back, got $x")
          }
          Numeric.toUnscaledString(value.value) shouldEqual Numeric.toUnscaledString(
            recoveredDecimal,
          )
        }
      }
    }

    "do Numeric" in {
      import ValueGenerators.Implicits._

      forAll("Numeric scale", "Decimal (BigDecimal) invariant") {
        (s: Numeric.Scale, d: BigDecimal) =>
          // we are filtering on decimals invariant under string conversion
          whenever(Numeric.fromBigDecimal(s, d).isRight) {
            val Right(dec) = Numeric.fromBigDecimal(s, d)
            val value = ValueNumeric(dec)
            val recoveredDecimal = ValueCoder.decodeValue[ContractId](
              defaultCidDecode,
              firstNumericVersion,
              assertRight(
                ValueCoder
                  .encodeValue[ContractId](defaultCidEncode, firstNumericVersion, value),
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
        testRoundTrip(value)
      }
    }

    "do Party" in {
      forAll(party) { p: Party =>
        val value = ValueParty(p)
        testRoundTrip(value)
      }
    }

    "do TimeStamp" in {
      forAll(timestampGen) { t: Time.Timestamp => // TODO: fails with Longs
        testRoundTrip(ValueTimestamp(t))
      }
    }

    "do Date" in {
      forAll(dateGen) { d: Time.Date =>
        testRoundTrip(ValueDate(d))
      }
    }

    "do ContractId" in {
      forAll(coidValueGen) { v: Value[ContractId] =>
        testRoundTrip(v)
      }
    }

    "do ContractId in any ValueVersion" in forAll(coidValueGen, valueVersionGen())(
      testRoundTripWithVersion,
    )

    "do lists" in {
      forAll(valueListGen) { v: ValueList[ContractId] =>
        testRoundTrip(v)
      }
    }

    "do optionals" in {
      forAll(valueOptionalGen) { v: ValueOptional[ContractId] =>
        testRoundTrip(v)
      }
    }

    "do maps" in {
      forAll(valueMapGen) { v: ValueTextMap[ContractId] =>
        testRoundTrip(v)
      }
    }

    "do genMaps" in {
      forAll(valueGenMapGen) { v: ValueGenMap[ContractId] =>
        testRoundTrip(v)
      }
    }

    "do variant" in {
      forAll(variantGen) { v: ValueVariant[ContractId] =>
        testRoundTrip(v)
      }
    }

    "do record" in {
      forAll(recordGen) { v: ValueRecord[ContractId] =>
        testRoundTrip(v)
      }
    }

    "don't struct" in {
      val struct = ValueStruct(ImmArray((Ref.Name.assertFromString("foo"), ValueInt64(42))))
      val res = ValueCoder.encodeValue[ContractId](defaultCidEncode, defaultValueVersion, struct)
      res.left.get.errorMessage should include("serializable")
    }

    "do unit" in {
      val recovered = ValueCoder.decodeValue(
        defaultCidDecode,
        defaultValueVersion,
        assertRight(
          ValueCoder.encodeValue[ContractId](defaultCidEncode, defaultValueVersion, ValueUnit),
        ),
      )
      val fromToBytes = ValueCoder.valueFromBytes(
        defaultCidDecode,
        ValueCoder.valueToBytes[ContractId](defaultCidEncode, ValueUnit).toOption.get,
      )
      Right(ValueUnit) shouldEqual fromToBytes
      recovered shouldEqual Right(ValueUnit)
    }

    "do identifier" in {
      forAll(idGen) { i =>
        ValueCoder.decodeIdentifier(ValueCoder.encodeIdentifier(i, None)._2) shouldEqual Right(i)
      }
    }

    "do identifier with supported override version" in forAll(idGen, valueVersionGen()) {
      (i, version) =>
        val (v2, ei) = ValueCoder.encodeIdentifier(i, Some(version))
        v2 shouldEqual version
        ValueCoder.decodeIdentifier(ei) shouldEqual Right(i)
    }

    "do versioned value with supported override version" in forAll(versionedValueGen) {
      case VersionedValue(version, value) => testRoundTripWithVersion(value, version)
    }

    "do versioned value with assigned version" in forAll(valueGen) { v: Value[ContractId] =>
      testRoundTripWithVersion(v, ValueVersions.assertAssignVersion(v))
    }

    "versioned value should pass serialization if unsupported override version provided" in
      forAll(valueGen, unsupportedValueVersionGen) {
        (value: Value[ContractId], badVer: ValueVersion) =>
          ValueVersions.acceptedVersions.contains(badVer) shouldBe false

          val actual: proto.VersionedValue = assertRight(
            ValueCoder
              .encodeVersionedValueWithCustomVersion(
                defaultCidEncode,
                VersionedValue(badVer, value),
              ),
          )

          actual.getVersion shouldEqual badVer.protoValue
      }

    "versioned value should fail deserialization if version is not supported" in
      forAll(valueGen, unsupportedValueVersionGen) {
        (value: Value[ContractId], badVer: ValueVersion) =>
          ValueVersions.acceptedVersions.contains(badVer) shouldBe false

          val protoWithUnsupportedVersion: proto.VersionedValue =
            assertRight(
              ValueCoder.encodeVersionedValueWithCustomVersion(
                defaultCidEncode,
                VersionedValue(badVer, value),
              ),
            )
          protoWithUnsupportedVersion.getVersion shouldEqual badVer.protoValue

          val actual: Either[DecodeError, VersionedValue[ContractId]] =
            ValueCoder.decodeVersionedValue(defaultCidDecode, protoWithUnsupportedVersion)

          actual shouldEqual Left(DecodeError(s"Unsupported value version ${badVer.protoValue}"))
      }
  }

  def testRoundTrip(value: Value[ContractId]): Assertion = {
    val recovered = ValueCoder.decodeValue(
      defaultCidDecode,
      defaultValueVersion,
      assertRight(ValueCoder.encodeValue[ContractId](defaultCidEncode, defaultValueVersion, value)),
    )
    val fromToBytes = ValueCoder.valueFromBytes(
      defaultCidDecode,
      assertRight(ValueCoder.valueToBytes[ContractId](defaultCidEncode, value)),
    )
    Right(value) shouldEqual recovered
    Right(value) shouldEqual fromToBytes
  }

  def testRoundTripWithVersion(value0: Value[ContractId], version: ValueVersion): Assertion = {
    ValueVersions.acceptedVersions.contains(version) shouldBe true

    val encoded: proto.VersionedValue = assertRight(
      ValueCoder
        .encodeVersionedValueWithCustomVersion(defaultCidEncode, VersionedValue(version, value0)),
    )
    val decoded: VersionedValue[ContractId] = assertRight(
      ValueCoder.decodeVersionedValue(defaultCidDecode, encoded),
    )

    decoded.value shouldEqual value0
    decoded.version shouldEqual version

    // emulate passing encoded proto message over wire

    val encodedSentOverWire: proto.VersionedValue =
      proto.VersionedValue.parseFrom(encoded.toByteArray)
    val decodedSentOverWire: VersionedValue[ContractId] = assertRight(
      ValueCoder.decodeVersionedValue(defaultCidDecode, encodedSentOverWire),
    )

    decodedSentOverWire.value shouldEqual value0
    decodedSentOverWire.version shouldEqual version
  }
}
