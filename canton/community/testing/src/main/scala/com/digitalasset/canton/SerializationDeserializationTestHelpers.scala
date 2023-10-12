// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.SerializationDeserializationTestHelpers.DefaultValueUntilExclusive
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString
import org.scalacheck.Arbitrary
import org.scalatest.Assertion
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

trait SerializationDeserializationTestHelpers extends BaseTest with ScalaCheckPropertyChecks {

  /*
   Test for classes extending `HasVersionedWrapper` (protocol version passed to the serialization method),
   without context for deserialization.
   */
  protected def testVersioned[T <: HasVersionedWrapper[_]](
      companion: HasVersionedMessageCompanion[T],
      defaults: List[DefaultValueUntilExclusive[T, _]] = Nil,
  )(implicit arb: Arbitrary[T]): Assertion =
    testVersionedCommon(companion, companion.fromByteString, defaults)

  /*
   Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
   without context for deserialization.
   */
  protected def testProtocolVersioned[T <: HasProtocolVersionedWrapper[
    T
  ], DeserializedValueClass <: HasRepresentativeProtocolVersion](
      companion: HasProtocolVersionedWrapperWithoutContextCompanion[T, DeserializedValueClass]
  )(implicit arb: Arbitrary[T]): Assertion =
    testProtocolVersionedCommon(companion, companion.fromByteString)

  /*
   Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
   with context for deserialization.
   */
  protected def testProtocolVersionedWithContext[T <: HasProtocolVersionedWrapper[
    T
  ], DeserializedValueClass <: HasRepresentativeProtocolVersion, Context](
      companion: HasMemoizedProtocolVersionedWithContextCompanion[T, Context],
      context: Context,
  )(implicit arb: Arbitrary[T]): Assertion =
    testProtocolVersionedCommon(companion, companion.fromByteString(context))

  /*
    Shared test code for classes extending `HasVersionedWrapper` (protocol version passed to the serialization method),
    with/without context for deserialization.
   */
  private def testVersionedCommon[T <: HasVersionedWrapper[_]](
      companion: HasVersionedMessageCompanionCommon[T],
      deserializer: ByteString => ParsingResult[_],
      defaults: List[DefaultValueUntilExclusive[T, _]],
  )(implicit arb: Arbitrary[T]): Assertion = {
    implicit val protocolVersionArb = GeneratorsVersion.protocolVersionArb

    forAll { (instance: T, protocolVersion: ProtocolVersion) =>
      val proto = instance.toByteString(protocolVersion)

      val deserializedInstance = clue(s"Deserializing serialized ${companion.name}")(
        deserializer(proto).value
      )

      val updatedInstance = defaults.foldLeft(instance) {
        case (instance, DefaultValueUntilExclusive(transformer, untilExclusive)) =>
          if (protocolVersion < untilExclusive) transformer(instance) else instance
      }

      withClue(
        s"Comparing ${companion.name} with (de)serialization done for pv=$protocolVersion"
      ) {
        updatedInstance shouldBe deserializedInstance
      }
    }
  }

  /*
     Shared test code for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
     with/without context for deserialization.
   */
  private def testProtocolVersionedCommon[T <: HasProtocolVersionedWrapper[
    T
  ], DeserializedValueClass <: HasRepresentativeProtocolVersion](
      companion: HasProtocolVersionedWrapperCompanion[T, DeserializedValueClass],
      deserializer: ByteString => ParsingResult[DeserializedValueClass],
  )(implicit arb: Arbitrary[T]): Assertion = {
    forAll { instance: T =>
      val proto = instance.toByteString

      val deserializedInstance = clue(s"Deserializing serialized ${companion.name}")(
        deserializer(proto).value
      )

      withClue(
        s"Comparing ${companion.name} with representative ${instance.representativeProtocolVersion}"
      ) {

        instance shouldBe deserializedInstance
        instance.representativeProtocolVersion shouldBe deserializedInstance.representativeProtocolVersion
      }
    }
  }
}

object SerializationDeserializationTestHelpers {
  final case class DefaultValueUntilExclusive[ValueClass, T](
      transformer: ValueClass => ValueClass,
      untilExclusive: ProtocolVersion,
  )
}
