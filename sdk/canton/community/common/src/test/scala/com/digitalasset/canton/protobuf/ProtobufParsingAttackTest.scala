// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protobuf

import cats.syntax.option.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.BufferException
import com.digitalasset.canton.serialization.ProtoConverter
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class ProtobufParsingAttackTest extends AnyWordSpec with BaseTest {

  "adding a field to a message definition" should {
    "ignore wrong field types" in {

      val attackAddField = AttackAddField("foo", 123).toByteString

      // Base parser can parse this
      ProtoConverter.protoParser(Base.parseFrom)(attackAddField) shouldBe
        Right(Base(Base.Sum.One("foo")))

      // Parser for the message after the field has been added will ignore the additional field
      ProtoConverter.protoParser(AddField.parseFrom)(attackAddField) shouldBe
        Right(AddField(AddField.Sum.One("foo"), None))
    }

    "explode when the field deserialization fails" in {
      val attackAddField =
        AttackAddFieldSameType("foo", ByteString.copyFromUtf8("BYTESTRING")).toByteString

      // Base parser can parse this
      ProtoConverter.protoParser(Base.parseFrom)(attackAddField) shouldBe
        Right(Base(Base.Sum.One("foo")))

      // Parser for the message after the field has been added will explode
      ProtoConverter
        .protoParser(AddField.parseFrom)(attackAddField)
        .left
        .value shouldBe a[BufferException]
    }
  }

  "adding an alternative to a one-of" should {
    "produce different parsing results" in {

      val dummyMessage = DummyMessage("dummy")
      val attackAddVariant = AttackAddVariant("bar", dummyMessage.toByteString).toByteString

      ProtoConverter.protoParser(Base.parseFrom)(attackAddVariant) shouldBe
        Right(Base(Base.Sum.One("bar")))

      ProtoConverter.protoParser(AddVariant.parseFrom)(attackAddVariant) shouldBe
        Right(AddVariant(AddVariant.Sum.Two(dummyMessage)))
    }

    "explode when given bad alternatives" in {
      val attackAddVariant =
        AttackAddVariant("bar", ByteString.copyFromUtf8("BYTESTRING")).toByteString

      ProtoConverter.protoParser(Base.parseFrom)(attackAddVariant) shouldBe
        Right(Base(Base.Sum.One("bar")))

      ProtoConverter
        .protoParser(AddVariant.parseFrom)(attackAddVariant)
        .left
        .value shouldBe a[BufferException]
    }
  }

  // This test shows that it should actually be fine to switch between `optional` and `repeated`
  // for fields in protobuf definitions. Our buf checks nevertheless forbid this because
  // buf doesn't offer a dedicated config option; FIELD_SAME_LABEL is needed for WIRE compatibility
  // and also complains about repeated, as explained in the docs.
  "repeating a structured field" should {
    "retain the last message" in {
      val dummyMessage1 = DummyMessage("dummy1")
      val dummyMessage2 = DummyMessage("dummy2")
      val repeated = Repeated(Seq("a", "b"), Seq(dummyMessage1, dummyMessage2)).toByteString

      ProtoConverter.protoParser(Single.parseFrom)(repeated) shouldBe
        Right(Single("b", dummyMessage2.some))
    }

    "explode when giving unparseable structured message" in {
      val dummyMessage = DummyMessage("dummy")
      val attackRepeated = AttackRepeated(
        Seq("a", "b"),
        Seq(ByteString.copyFromUtf8("NOT-A-DUMMY-MESSAGE"), dummyMessage.toByteString),
      ).toByteString

      ProtoConverter
        .protoParser(Single.parseFrom)(attackRepeated)
        .left
        .value shouldBe a[BufferException]

      ProtoConverter
        .protoParser(Repeated.parseFrom)(attackRepeated)
        .left
        .value shouldBe a[BufferException]
    }
  }

  // The JVM does not have unsigned ints (except for char), so protobuf uintX are mapped to signed numbers
  // in the generated Scala classes. This test here makes sure that we detect if the generated parsers
  // change this behavior.
  "Protobuf does not distinguish between signed and unsigned varints" in {
    val signedInt = SignedInt(-1)
    ProtoConverter.protoParser(UnsignedInt.parseFrom)(signedInt.toByteString) shouldBe
      Right(UnsignedInt(-1))
  }

  // This test demonstrates that it is not safe to change the range of a varint in a protobuf message
  "silently truncate numbers to smaller range" in {
    val unsignedLong = UnsignedLong(0x7fffffffffffffffL)
    ProtoConverter.protoParser(UnsignedInt.parseFrom)(unsignedLong.toByteString) shouldBe
      Right(UnsignedInt(-1))
  }
}
