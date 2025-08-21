// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{LoggerUtil, ReassignmentTag}
import com.google.protobuf.ByteString
import org.reflections.Reflections
import org.scalacheck.Arbitrary
import org.scalatest.Assertion
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.lang.reflect.Modifier
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

import SerializationDeserializationTestHelpers.*

trait SerializationDeserializationTestHelpers extends BaseTest with ScalaCheckPropertyChecks {

  // Classes for which we ran the (de)serialization tests
  // Populated by the methods `testVersioned` and friends
  lazy val testedClasses: scala.collection.mutable.Set[String] = mutable.Set.empty

  /** Test for classes extending `HasVersionedWrapper` (protocol version passed to the serialization
    * method), without context for deserialization.
    */
  protected def testVersioned[T <: HasVersionedWrapper[_]](
      companion: HasVersionedMessageCompanion[T],
      protocolVersion: ProtocolVersion,
      defaults: List[DefaultValueUntilExclusive[T]] = Nil,
  )(implicit arb: Arbitrary[T]): Assertion =
    testVersionedCommon(companion, protocolVersion, companion.fromTrustedByteString, defaults)

  /** Test for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the
    * instance). In case the test runs slow or becomes flaky, set warnWhenTestRunsLongerThan to 1.5x
    * to 2x the normal runtime.
    */
  protected def test[
      T <: HasProtocolVersionedWrapper[T],
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
  ](
      companion: BaseVersioningCompanion[
        T,
        Unit,
        DeserializedValueClass,
        Unit,
      ],
      protocolVersion: ProtocolVersion,
      warnWhenTestRunsLongerThan: Duration = 1.second,
  )(implicit arb: Arbitrary[T]): Assertion =
    testProtocolVersionedCommon(
      companion,
      companion.fromByteString(protocolVersion, ()),
      warnWhenTestRunsLongerThan,
    )

  /** In case the test runs slow or becomes flaky, set warnWhenTestRunsLongerThan to 1.5x to 2x the
    * normal runtime.
    */
  protected def testContext[
      T <: HasProtocolVersionedWrapper[T],
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
      Context,
      Dependency,
  ](
      companion: BaseVersioningCompanion[T, Context, DeserializedValueClass, Dependency],
      context: Context,
      protocolVersion: ProtocolVersion,
      warnWhenTestRunsLongerThan: Duration = 2.second,
  )(implicit arb: Arbitrary[T]): Assertion =
    testProtocolVersionedCommon(
      companion,
      companion.fromByteString(protocolVersion, context),
      warnWhenTestRunsLongerThan,
    )

  /** In case the test runs slow or becomes flaky, set warnWhenTestRunsLongerThan to 1.5x to 2x the
    * normal runtime.
    */
  protected def testContextTaggedProtocolVersion[
      ValueClass <: HasProtocolVersionedWrapper[ValueClass],
      T[X] <: ReassignmentTag[X],
      Context,
  ](
      companion: VersioningCompanionContextTaggedPVValidation2[ValueClass, T, Context],
      context: Context,
      protocolVersion: T[ProtocolVersion],
      warnWhenTestRunsLongerThan: Duration = 1.second,
  )(implicit arb: Arbitrary[ValueClass]): Assertion =
    testProtocolVersionedCommon(
      companion,
      companion.fromByteString(context, protocolVersion),
      warnWhenTestRunsLongerThan,
    )

  /*
    Shared test code for classes extending `HasVersionedWrapper` (protocol version passed to the serialization method),
    with/without context for deserialization.
   */
  private def testVersionedCommon[T <: HasVersionedWrapper[_]](
      companion: HasVersionedMessageCompanionCommon[T],
      protocolVersion: ProtocolVersion,
      deserializer: ByteString => ParsingResult[_],
      defaults: List[DefaultValueUntilExclusive[T]],
  )(implicit arb: Arbitrary[T]): Assertion =
    forAll { (instance: T) =>
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

  /*
     Shared test code for classes extending `HasProtocolVersionedWrapper` (protocol version embedded in the instance),
     with/without context for deserialization.
   */
  private def testProtocolVersionedCommon[
      T <: HasProtocolVersionedWrapper[T],
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
  ](
      companion: BaseVersioningCompanion[T, ?, DeserializedValueClass, ?],
      deserializer: ByteString => ParsingResult[DeserializedValueClass],
      warnWhenTestRunsLongerThan: Duration,
  )(implicit arb: Arbitrary[T]): Assertion = {
    val className = companion.getClass.getName
    testedClasses.add(className)

    val start = System.nanoTime()
    val result = forAll { (instance: T) =>
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
    val elapsed = Duration.fromNanos(System.nanoTime() - start)

    if (elapsed > warnWhenTestRunsLongerThan)
      logger.warn(
        s"Test for $className took ${LoggerUtil.roundDurationForHumans(elapsed)} to run, instead of the allotted $warnWhenTestRunsLongerThan.\n" +
          "Likely causes: Generating unbounded lists, getting a quadratic size from combining bounded, nested lists, ..."
      )
    result
  }

  protected def findBaseVersionionCompanionSubClasses(): Seq[Class[?]] =
    findSubClassesOf(classOf[BaseVersioningCompanion[_, _, _, _]])
}

object SerializationDeserializationTestHelpers {
  final case class DefaultValueUntilExclusive[ValueClass](
      transformer: ValueClass => ValueClass,
      untilExclusive: ProtocolVersion,
  )

  /* Find all subclasses of `parent` com.digitalasset.canton */
  def findSubClassesOf[T](parent: Class[T]): Seq[Class[_ <: T]] = {
    val reflections = new Reflections("com.digitalasset.canton")

    val classes: Seq[Class[_ <: T]] =
      reflections.getSubTypesOf(parent).asScala.toSeq

    // Exclude abstract classes as they cannot be true companion objects, but rather just helper traits
    def isAbstract(c: Class[_]): Boolean = Modifier.isAbstract(c.getModifiers)
    // Exclude anonymous companion objects as they should only appear in tests
    def isAnonymous(c: Class[_]): Boolean = c.isAnonymousClass

    classes.filterNot(c => isAbstract(c) || isAnonymous(c))
  }
}

/** Marker trait for companion objects of [[HasProtocolVersionedWrapper]] classes that we don't need
  * to check serialization/deserialization for. Since this trait is defined in the test scope, it
  * cannot be used for classes in production code.
  */
trait IgnoreInSerializationTestExhaustivenessCheck
