// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.{
  AbstractLengthLimitedString,
  LengthLimitedStringCompanion,
}
import com.digitalasset.canton.version.IgnoreInSerializationTestExhaustivenessCheck
import com.google.protobuf.ByteString
import org.reflections.Reflections
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*

object Generators {
  private val containerMaxSize: Int = 4

  implicit val byteStringArb: Arbitrary[ByteString] = Arbitrary(
    Gen.stringOfN(256, Gen.alphaNumChar).map(ByteString.copyFromUtf8)
  )

  implicit val userIdArb: Arbitrary[UserId] = Arbitrary(
    Gen.stringOfN(32, Gen.alphaNumChar).map(UserId.assertFromString)
  )
  implicit val commandIdArb: Arbitrary[CommandId] = Arbitrary(
    Gen.stringOfN(32, Gen.alphaNumChar).map(CommandId.assertFromString)
  )
  implicit val ledgerSubmissionIdArb: Arbitrary[LedgerSubmissionId] = Arbitrary(
    Gen.stringOfN(32, Gen.alphaNumChar).map(LedgerSubmissionId.assertFromString)
  )
  implicit val workflowIdArb: Arbitrary[WorkflowId] = Arbitrary(
    Gen.stringOfN(32, Gen.alphaNumChar).map(WorkflowId.assertFromString)
  )

  def reassignmentCounterGen: Gen[ReassignmentCounter] =
    Gen.choose(0, Long.MaxValue).map(i => ReassignmentCounter(i))

  def lengthLimitedStringGen[A <: AbstractLengthLimitedString](
      companion: LengthLimitedStringCompanion[A]
  ): Gen[A] = for {
    length <- Gen.choose(1, companion.maxLength.unwrap)
    str <- Gen.stringOfN(length, Gen.alphaNumChar)
  } yield companion.tryCreate(str)

  def nonEmptyListGen[T](implicit arb: Arbitrary[T]): Gen[NonEmpty[List[T]]] = for {
    size <- Gen.choose(1, containerMaxSize - 1)
    element <- arb.arbitrary
    elements <- Gen.containerOfN[List, T](size, arb.arbitrary)
  } yield NonEmpty(List, element, elements*)

  def nonEmptySetGen[T](implicit arb: Arbitrary[T]): Gen[NonEmpty[Set[T]]] =
    nonEmptyListGen[T].map(_.toSet)
  def nonEmptySet[T](implicit arb: Arbitrary[T]): Arbitrary[NonEmpty[Set[T]]] =
    Arbitrary(nonEmptyListGen[T].map(_.toSet))

  def boundedListGen[T](implicit arb: Arbitrary[T]): Gen[List[T]] =
    boundedListGen(arb.arbitrary)

  def boundedListGen[T](gen: Gen[T]): Gen[List[T]] =
    Gen.containerOfN[List, T](containerMaxSize, gen)

  def boundedSetGen[T](implicit arb: Arbitrary[T]): Gen[Set[T]] =
    Gen.containerOfN[Set, T](containerMaxSize, arb.arbitrary)

  def boundedMapGen[K, V](implicit arb: Arbitrary[(K, V)]): Gen[Map[K, V]] =
    Gen.mapOfN(containerMaxSize, arb.arbitrary)

  sealed trait GeneratorForClass {
    type Typ <: AnyRef
    def gen: Gen[Typ]
    def clazz: Class[Typ]
  }
  object GeneratorForClass {
    def apply[T <: AnyRef](generator: Gen[T], claz: Class[T]): GeneratorForClass { type Typ = T } =
      new GeneratorForClass {
        override type Typ = T
        override def gen: Gen[Typ] = generator
        override def clazz: Class[Typ] = claz
      }
  }

  def arbitraryForAllSubclasses[T](clazz: Class[T])(
      g0: GeneratorForClass { type Typ <: T },
      moreGens: GeneratorForClass { type Typ <: T }*
  ): Arbitrary[T] = {
    val availableGenerators = (g0 +: moreGens).map(_.clazz)

    val coveredClasses =
      availableGenerators.flatMap { gen =>
        /*
        We consider that a generator for a sub-class `C` is exhaustive for all its sub-classes.
        This has to be ensured another way.
         */
        val coveredSubClasses = findSubClassesOf(gen).map(_.getName)

        gen.getName +: coveredSubClasses // a generator for C covers C
      }.toSet

    val foundSubClasses = findSubClassesOf(clazz)
      .map(_.getName)
      .toSet

    val foundLeavesSubClasses = findSubClassesOf(clazz)
      .filter { clazz =>
        findSubClassesOf(clazz).isEmpty // leaves only
      }
      .map(_.getName)
      .toSet

    val missingGeneratorsForSubclasses = foundLeavesSubClasses -- coveredClasses -- ignoredClasses
    require(
      missingGeneratorsForSubclasses.isEmpty,
      s"No generators for subclasses $missingGeneratorsForSubclasses of ${clazz.getName}",
    )
    val redundantGenerators = coveredClasses -- foundSubClasses

    require(
      redundantGenerators.isEmpty,
      s"Reflection did not find the subclasses $redundantGenerators of ${clazz.getName}",
    )

    val allGen = NonEmpty.from(moreGens) match {
      case None => g0.gen
      case Some(moreGensNE) =>
        Gen.oneOf[T](
          g0.gen,
          moreGensNE.head1.gen,
          moreGensNE.tail1.map(_.gen) *,
        )
    }
    // Returns an Arbitrary instead of a Gen so that the exhaustiveness check is forced upon creation
    // rather than first usage of the Arbitrary's arbitrary.
    Arbitrary(allGen)
  }

  lazy val reflections = new Reflections("com.digitalasset.canton")
  private lazy val ignoredClasses: Set[String] =
    findSubClassesOf(classOf[IgnoreInSerializationTestExhaustivenessCheck]).map(_.getName).toSet

  private val subClassesCache: TrieMap[String, Seq[Class[?]]] = TrieMap.empty

  /* Find all subclasses of `parent` */
  private def findSubClassesOf[T](parent: Class[T]): Seq[Class[?]] =
    subClassesCache.getOrElseUpdate(
      parent.getName, {
        val classes: Seq[Class[? <: T]] =
          reflections.getSubTypesOf(parent).asScala.toSeq

        // Exclude anonymous companion objects as they should only appear in tests
        def isAnonymous(c: Class[?]): Boolean = c.isAnonymousClass

        classes.filterNot(isAnonymous)
      },
    )
}
