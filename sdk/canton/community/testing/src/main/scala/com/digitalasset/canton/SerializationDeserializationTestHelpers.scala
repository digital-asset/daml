// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.SerializationDeserializationTestHelpers.DefaultValueUntilExclusive
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString
import org.reflections.Reflections
import org.scalacheck.Arbitrary
import org.scalatest.Assertion
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

trait SerializationDeserializationTestHelpers extends BaseTest with ScalaCheckPropertyChecks {

  // Classes for which we ran the (de)serialization tests
  // Populated by the methods `testVersioned` and friends
  lazy val testedClasses: scala.collection.mutable.Set[String] = mutable.Set.empty

  /*
   Test for classes extending `HasVersionedWrapper` (protocol version passed to the serialization method),
   without context for deserialization.
   */
  protected def testVersioned[T <: HasVersionedWrapper[_]](
      companion: HasVersionedMessageCompanion[T],
      defaults: List[DefaultValueUntilExclusive[T]] = Nil,
  )(implicit arb: Arbitrary[T]): Assertion =
    testVersionedCommon(companion, companion.fromTrustedByteString, defaults)

  /*
   Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
   without context for deserialization.

   No deserialization validation (`fromByteStringUnsafe`) because the protocol version of the generated
   instances is unpredictable.
   */
  protected def testProtocolVersioned[T <: HasProtocolVersionedWrapper[
    T
  ], DeserializedValueClass <: HasRepresentativeProtocolVersion](
      companion: HasProtocolVersionedWrapperWithoutContextCompanion[T, DeserializedValueClass]
  )(implicit arb: Arbitrary[T]): Assertion = {
    testProtocolVersionedCommon(
      companion,
      companion.fromTrustedByteString,
    )
  }

  /*
   Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
   with memoization and without context for deserialization.

   No deserialization validation (`fromByteStringUnsafe`) because the protocol version of the generated
  instances is unpredictable.
   */
  protected def testMemoizedProtocolVersioned[T <: HasProtocolVersionedWrapper[T]](
      companion: HasMemoizedProtocolVersionedWrapperCompanion[T]
  )(implicit arb: Arbitrary[T]): Assertion = {
    testProtocolVersionedCommon(
      companion,
      companion.fromTrustedByteString,
    )
  }
  protected def testMemoizedProtocolVersioned2[T <: HasProtocolVersionedWrapper[
    T
  ], U <: HasProtocolVersionedWrapper[?]](
      companion: HasMemoizedProtocolVersionedWrapperCompanion2[T, U]
  )(implicit arb: Arbitrary[T]): Assertion = {
    testProtocolVersionedCommon(
      companion,
      companion.fromTrustedByteString,
    )
  }

  /*
   Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
   with memoization and context for deserialization.

   No deserialization validation (`fromByteStringUnsafe`) because the protocol version of the generated
  instances is unpredictable.
   */
  protected def testMemoizedProtocolVersionedWithCtx[T <: HasProtocolVersionedWrapper[T], Context](
      companion: HasMemoizedProtocolVersionedWithContextCompanion[T, Context],
      context: Context,
  )(implicit arb: Arbitrary[T]): Assertion =
    testProtocolVersionedCommon(
      companion,
      companion.fromTrustedByteString(context),
    )

  /*
 Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
 with memoization and context and validation for deserialization.

 No deserialization validation (`fromByteStringUnsafe`) because the protocol version of the generated
  instances is unpredictable.
   */
  protected def testProtocolVersionedWithCtxAndValidation[T <: HasProtocolVersionedWrapper[
    T
  ], Context](
      companion: HasProtocolVersionedWithContextAndValidationCompanion[T, Context],
      context: Context,
      protocolVersion: ProtocolVersion,
  )(implicit arb: Arbitrary[T]): Assertion =
    testProtocolVersionedCommon(
      companion,
      companion.fromByteString(context, protocolVersion),
    )

  /*
   Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
   with context for deserialization.

   No deserialization validation (`fromByteStringUnsafe`) because the protocol version of the generated
  instances is unpredictable.
   */
  protected def testProtocolVersionedWithCtx[T <: HasProtocolVersionedWrapper[
    T
  ], DeserializedValueClass <: HasRepresentativeProtocolVersion, Context](
      companion: HasProtocolVersionedWithContextCompanion[T, Context],
      context: Context,
  )(implicit arb: Arbitrary[T]): Assertion =
    testProtocolVersionedCommon(
      companion,
      companion.fromTrustedByteString(context),
    )

  /*
    Shared test code for classes extending `HasVersionedWrapper` (protocol version passed to the serialization method),
    with/without context for deserialization.
   */
  private def testVersionedCommon[T <: HasVersionedWrapper[_]](
      companion: HasVersionedMessageCompanionCommon[T],
      deserializer: ByteString => ParsingResult[_],
      defaults: List[DefaultValueUntilExclusive[T]],
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
    testedClasses.add(companion.getClass.getName.replace("$", ""))

    forAll { (instance: T) =>
      val proto = clue(s"Serializing instance of ${companion.name}")(instance.toByteString)

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

  /* Find all subclasses of `parent` in package `packageName` */
  private def findSubClassesOf[T](parent: Class[T], packageName: String) = {
    val reflections = new Reflections(packageName)

    val classes: Seq[Class[_ <: T]] =
      reflections.getSubTypesOf(parent).asScala.toList.filterNot(_.getName.contains("$"))

    // Check if one superclass of `c` is also in the list of classes
    def hasParent(c: Class[_ <: T]) =
      classes.exists(p => p.getName != c.getName && p.isAssignableFrom(c))

    classes.filterNot(hasParent)
  }

  protected def findHasProtocolVersionedWrapperSubClasses(packageName: String): Seq[String] =
    findSubClassesOf(classOf[HasProtocolVersionedWrapper[_]], packageName).map(_.getName)
}

object SerializationDeserializationTestHelpers {
  final case class DefaultValueUntilExclusive[ValueClass](
      transformer: ValueClass => ValueClass,
      untilExclusive: ProtocolVersion,
  )
}
