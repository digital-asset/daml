// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf
package value

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.{Versioned, SerializationVersion}
import com.digitalasset.daml.lf.value.{ValueOuterClass => proto}
import org.scalacheck.{Shrink, Arbitrary}
import org.scalatest.{Assertion, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ValueCoderSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with EitherAssertions
    with ScalaCheckPropertyChecks {

  import Value._
  import test.ValueGenerators._

  implicit val noStringShrink: Shrink[String] = Shrink.shrinkAny[String]

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 1000)

  "encode" should {
    "fail gracefully when serialized message exceeding 2GB" in {

      val ver = SerializationVersion.StableVersions.max

      // We maximize structural sharing to limit the JVM heap memory usage.
      // This allows us to create objects that would serialize to massive sizes (e.g. > 512MB or 4GB)
      // without actually consuming that much RAM.
      val values = LazyList.iterate[Value](ValueText("a" * 1024))(x => ValueList(FrontStack(x, x)))

      // values(19), and  values(20) are technically serializable, but their serialization requires over 500 MB of memory.
      // We skip it to avoid excessive memory pressure on CI agents.
      val value18 = values(18) // serialization needs a bit more than 256MB
      val value21 = values(21) // serialization would need a bit more than 2GB
      val value22 = values(22) // serialization would need a bit more than 4GB

      ValueCoder.encodeValue(valueVersion = ver, v0 = value18) shouldBe a[Right[_, _]]
      ValueCoder.encodeValue(valueVersion = ver, v0 = value21) shouldBe a[Left[_, _]]
      ValueCoder.encodeValue(valueVersion = ver, v0 = value22) shouldBe a[Left[_, _]]
    }

    val valuesWithNullCharacters = Table(
      "LF value with null character",
      ValueText("->\u0000<-"),
      ValueOptional(Some(ValueText("\u0000"))),
      ValueTextMap(SortedLookupList(Map("key\u0000" -> ValueInt64(0)))),
      ValueTextMap(
        SortedLookupList(Map("key\u0001" -> ValueInt64(1), "key\u0001\u0000" -> ValueInt64(2)))
      ),
    )

    "fail gracefully on texts with null characters" in {
      forAll(valuesWithNullCharacters) { v =>
        inside(ValueCoder.encodeValue(valueVersion = SerializationVersion.minVersion, v0 = v)) {
          case Left(ValueCoder.EncodeError(msg)) =>
            msg.toLowerCase() should include("null character")
        }
      }
    }

    "accepts texts with null characters when allowNullCharacter flag is set" in {
      val ValueCoder = new lf.value.ValueCoder(allowNullCharacters = true).internal

      forAll(valuesWithNullCharacters) { v =>
        ValueCoder.encodeValue(valueVersion = SerializationVersion.minVersion, v0 = v) shouldBe a[
          Right[_, _]
        ]
      }
    }
  }

  "decode" should {

    val protoWithNullCharacters = Table(
      "Proto value with null character",
      proto.Value.newBuilder().setText("->\u0000<-").build(),
      proto.Value
        .newBuilder()
        .setList(
          proto.Value.List
            .newBuilder()
            .addElements(
              proto.Value.newBuilder().setText("\u0001\u0000")
            )
        )
        .build(),
      proto.Value
        .newBuilder()
        .setTextMap(
          proto.Value.TextMap
            .newBuilder()
            .addEntries(
              proto.Value.TextMap.Entry
                .newBuilder()
                .setKey("\u0000")
                .setValue(proto.Value.newBuilder().setInt64(0L))
            )
        )
        .build(),
    )

    "fail gracefully on texts null characters" in {
      forEvery(protoWithNullCharacters)(v =>
        inside(ValueCoder.decodeValue(SerializationVersion.minVersion, v.toByteString)) {
          case Left(ValueCoder.DecodeError(msg)) =>
            msg.toLowerCase() should include("null character")
        }
      )
    }

    "accepts texts with null characters when allowNullCharacter flag is set" in {

      val ValueCoder = new lf.value.ValueCoder(allowNullCharacters = true).internal

      forEvery(protoWithNullCharacters)(v =>
        ValueCoder
          .decodeValue(SerializationVersion.minVersion, v.toByteString) shouldBe a[Right[_, _]]
      )
    }

  }

  "encode-decode" should {
    "do Int" in {
      forAll(Arbitrary.arbLong.arbitrary, SerializationVersionGen())((i, v) =>
        testRoundTrip(ValueInt64(i), v)
      )
    }

    "do Bool" in {
      forAll(Arbitrary.arbBool.arbitrary, SerializationVersionGen())((b, v) =>
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
            val recoveredNumeric = ValueCoder.decodeValue(
              version = SerializationVersion.minVersion,
              bytes = assertRight(
                ValueCoder
                  .encodeValue(
                    valueVersion = SerializationVersion.minVersion,
                    v0 = value,
                  )
              ),
            ) match {
              case Right(ValueNumeric(x)) => x
              case x => fail(s"should have got a numeric back, got $x")
            }
            Numeric.toUnscaledString(value.value) shouldEqual Numeric.toUnscaledString(
              recoveredNumeric
            )
          }
      }
    }

    "do Text" in {
      forAll("Text (String) invariant", SerializationVersionGen())((t, v) =>
        testRoundTrip(ValueText(t), v)
      )
    }

    "do Party" in {
      forAll(party, SerializationVersionGen())((p, v) => testRoundTrip(ValueParty(p), v))
    }

    "do TimeStamp" in {
      forAll(timestampGen, SerializationVersionGen())((t, v) => testRoundTrip(ValueTimestamp(t), v))
    }

    "do Date" in {
      forAll(dateGen, SerializationVersionGen())((d, v) => testRoundTrip(ValueDate(d), v))
    }

    "do ContractId" in {
      forAll(coidValueGen, SerializationVersionGen())(testRoundTrip)
    }

    "do ContractId V0 in any ValueVersion" in forAll(coidValueGen, SerializationVersionGen())(
      testRoundTrip
    )

    "do lists" in {
      forAll(valueListGen, SerializationVersionGen())(testRoundTrip)
    }

    "do optionals" in {
      forAll(valueOptionalGen, SerializationVersionGen())(testRoundTrip)
    }

    "do textMaps" in {
      forAll(valueTextMapGen, SerializationVersionGen())(testRoundTrip)
    }

    "do genMaps" in {
      forAll(valueGenMapGen, SerializationVersionGen())(testRoundTrip)
    }

    "do variant" in {
      forAll(variantGen, SerializationVersionGen())(testRoundTrip)
    }

    "do record" in {
      forAll(recordGen, SerializationVersionGen())(testRoundTrip)
    }

    "do unit" in {
      forAll(SerializationVersionGen())(testRoundTrip(ValueUnit, _))
    }

    "do identifier" in {
      forAll(idGen) { i =>
        ValueCoder.decodeIdentifier(ValueCoder.encodeIdentifier(i)) shouldEqual Right(i)
      }
    }

    "do identifier with supported override version" in forAll(idGen, SerializationVersionGen()) {
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
          valueVersion = SerializationVersion.maxVersion,
          v0 = toNat(1, n), // 101
        ) shouldBe a[Left[_, _]]

      val encoded = assertRight(
        ValueCoder
          .encodeValue(valueVersion = SerializationVersion.maxVersion, v0 = n)
      )

      ValueCoder.decodeValue(
        version = SerializationVersion.maxVersion,
        bytes = encoded,
      ) shouldBe Right(n)
    }
  }

  def testRoundTrip(value0: Value, version: SerializationVersion): Assertion = {
    val normalizedValue = transaction.Util.assertNormalizeValue(value0, version)
    val encoded: proto.VersionedValue = assertRight(
      ValueCoder
        .encodeVersionedValue(versionedValue = Versioned(version, value0))
    )
    val decoded: VersionedValue = assertRight(
      ValueCoder.decodeVersionedValue(protoValue0 = encoded)
    )

    decoded shouldEqual Versioned(version, normalizedValue)

    // emulate passing encoded proto message over wire

    val encodedSentOverWire: proto.VersionedValue =
      proto.VersionedValue.parseFrom(encoded.toByteArray)
    val decodedSentOverWire: VersionedValue = assertRight(
      ValueCoder.decodeVersionedValue(protoValue0 = encodedSentOverWire)
    )

    decodedSentOverWire shouldEqual Versioned(version, normalizedValue)
  }
}
