// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.speedy.CostModel.CostModelV0
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.test.ValueGenerators
import com.google.protobuf
import org.openjdk.jol.info.GraphLayout
import org.scalacheck.Gen
import org.scalacheck.util.Buildable
import org.scalactic.anyvals.PosInt
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.immutable.{ArraySeq, TreeMap}
import scala.reflect.ClassTag

class CostModelV0Test extends AnyWordSpec with Matchers with ScalaCheckPropertyChecks {

  import CostModelTest._

  def test[T](
      name: String,
      strict: Boolean = false,
      verbose: Boolean = true,
      minSucc: PosInt = 10,
  )(implicit sized: Sized[T], gen: Gen[T]): Unit =
    s"approximate real footprint of $name" in {
      forAll(gen, minSuccessful(minSucc)) { x: T =>
        sized.bloat(x)

        val shallowFootprint = sized.shallowFootprint(x)
        val approximativeShallowSize = sized.approximateShallowFootprint(x)

        val s = compare(shallowFootprint, approximativeShallowSize)
        println(
          s"Sized: ${name}, actualShallowSize: $shallowFootprint, approximativeShallowSize: $approximativeShallowSize ($s), ${compare(shallowFootprint, approximativeShallowSize)}, x=`${x.toString
              .take(60)}`"
        )
        if (verbose) {
          println(classLayout(x))
          println(graphLayout(x))
        }

        if (shallowFootprint > approximativeShallowSize)
          if (strict)
            shallowFootprint shouldBe approximativeShallowSize
          else
            shallowFootprint should be <= approximativeShallowSize

        Sized.footprint(x) should be <= sized.approximateFootprint(x)
      }
    }

  "Sized#approximateFootprint" should {

    /* Generic atom */
    test[MyLong]("MyLong")
    test[Array[Byte]]("Array[Byte]", minSucc = 100)
    test[String]("String", minSucc = 100)

    test[protobuf.ByteString]("BytesString", verbose = true)

    /* Generic data structures */
    test[Option[MyLong]]("Option[MyLong]")
    test[List[MyLong]]("List[MyLong]", minSucc = 100)
    test[Array[MyLong]]("Array[MyLong]", minSucc = 100)

    test[Option[String]]("Option[String]", strict = Sized.precise, minSucc = 100)
    test[List[String]](
      "List[String]",
      strict = Sized.precise,
      minSucc = 100,
    )
    test[Array[String]]("Array[String]", strict = Sized.precise)

    /* LF data structures */

    test[Bytes]("Bytes", strict = Sized.precise)
    test[crypto.Hash]("Hash", strict = false)
    test[Numeric]("Numeric", strict = false, minSucc = 100)
    test[Time.Date]("Date", strict = true)
    test[Time.Timestamp]("Timestamp", strict = true)

    test[ImmArray[MyLong]]("ImmArray[MyInt]", strict = true)
    test[FrontStack[MyLong]]("FrontStack[MyInt]", strict = true)

    test[ImmArray[String]]("ImmArray[String]", strict = Sized.precise)
    test[FrontStack[String]]("FrontStack[String]", strict = Sized.precise)

    /* Speedy data structures */

    test[SValue.SUnit.type]("SUnit", strict = true, minSucc = 1)

    test[SValue.SBool]("SBool", minSucc = 2)(Sized.SizedSBool, genSBool)
    test[SValue.SInt64]("SInt64", strict = true)
    test[SValue.SDate](name = "SDate", strict = true)
    test[SValue.STimestamp](name = "STimestamp", strict = true)
    test[SValue.SText](name = "SText", strict = true)
    test[SValue.SNumeric](name = "SNumeric", strict = true)

  }
}

object CostModelTest {

  import Sized._

  def graphLayout(x: Any) =
    org.openjdk.jol.info.GraphLayout.parseInstance(x).toFootprint

  def classLayout(x: Any) =
    org.openjdk.jol.info.ClassLayout.parseInstance(x).toPrintable

  case class MyLong(x: Long)

  def compare(x: Long, y: Long) =
    "%.2f%%".format(((x.toDouble - y.toDouble) / x.toDouble * 100))

  object MyLong {
    implicit val ordering: Ordering[MyLong] = Ordering.by[MyLong, Long](_.x)
    implicit val gen: Gen[MyLong] = Gen.long.map(MyLong(_))
    implicit val sized: Sized[MyLong] = new SizedFixSizeAtom[MyLong](24)
  }

  implicit def buildableArray[T: ClassTag]: Buildable[T, Array[T]] =
    new Buildable[T, Array[T]] {
      def builder = Array.newBuilder[T]
    }

  implicit def genByte: Gen[Byte] = Gen.chooseNum(Byte.MinValue, Byte.MaxValue)

  def genByteArrayN(n: Int) = Gen.containerOfN(n, genByte)

  implicit def genByteArray: Gen[Array[Byte]] =
    Gen.sized(s => Gen.choose(0, s max 0)).flatMap(genByteArrayN)

  implicit def genString: Gen[String] = Gen.alphaStr.map {
    case "" => String.valueOf(Array.empty) // avoid sharing of empty string
    case s => s
  }

  implicit def genOption[X](implicit x: Gen[X]): Gen[Option[X]] = Gen.option(x)

  implicit def genList[X](implicit x: Gen[X]): Gen[List[X]] = Gen.listOf(x)

  implicit def genArray[X: ClassTag](implicit x: Gen[X]): Gen[Array[X]] = Gen.containerOf(x)

  implicit def genTreeMap[K, V](implicit
      k: Gen[K],
      v: Gen[V],
      ordering: Ordering[K],
  ): Gen[TreeMap[K, V]] =
    Gen.listOf(for { x <- k; y <- v } yield (x, y)).map(_.to(TreeMap))

  implicit def genImmArray[X](implicit x: Gen[X]): Gen[ImmArray[X]] =
    Gen.listOf(x).map(ImmArray.from(_))

  implicit def genFrontStack[X](implicit x: Gen[X]): Gen[FrontStack[X]] =
    Gen.listOf(x).map(_.foldLeft(FrontStack.empty[X])(_.+:(_)))

  implicit def genBackStack[X](implicit x: Gen[X]): Gen[BackStack[X]] =
    Gen.listOf(x).map(_.foldLeft(BackStack.empty[X])(_.:+(_)))

  implicit def genNumeric: Gen[Numeric] = ValueGenerators.unscaledNumGen

  implicit def genDate: Gen[Time.Date] = ValueGenerators.dateGen

  implicit def genTimeStamp: Gen[Time.Timestamp] = ValueGenerators.timestampGen

  implicit def genByteString: Gen[protobuf.ByteString] =
    genByteArray.map(protobuf.ByteString.copyFrom(_))

  implicit def genBytes: Gen[Bytes] =
    genByteString.map(Bytes.fromByteString)

  implicit def genHash: Gen[crypto.Hash] =
    genByteArrayN(32).map(crypto.Hash.assertFromByteArray)

  def genParty: Gen[Ref.Party] = ValueGenerators.party

  def genName: Gen[Ref.Name] = ValueGenerators.nameGen

  implicit def genDottedName: Gen[Ref.DottedName] = ValueGenerators.dottedNameGen

  implicit def genQualifiedName: Gen[Ref.QualifiedName] = genIdentifier.map(_.qualifiedName)

  implicit def genIdentifier: Gen[Ref.Identifier] = ValueGenerators.idGen

  implicit def genContractIdV1: Gen[Value.ContractId.V1] = for {
    discriminator <- genHash
    suffixLength <- Gen.choose(0, 0)
    suffix <- genByteArrayN(suffixLength)
  } yield Value.ContractId.V1(discriminator, Bytes.fromByteArray(suffix))

  implicit lazy val genSValue: Gen[SValue] =
    Gen.oneOf(genSBool, genSBool)

  implicit val genSUnit: Gen[SValue.SUnit.type] = Gen.const(SValue.SUnit)
  implicit val genSBool: Gen[SValue.SBool] = Gen.oneOf(true, false).map(SValue.SBool(_))
  implicit val genSInt64: Gen[SValue.SInt64] = Gen.long.map(SValue.SInt64(_))
  implicit val genSDate: Gen[SValue.SDate] = genDate.map(SValue.SDate)
  implicit val genSTimestamp: Gen[SValue.STimestamp] = genTimeStamp.map(SValue.STimestamp)
  implicit val genSNumeric: Gen[SValue.SNumeric] = genNumeric.map(SValue.SNumeric(_))
  implicit val genSText: Gen[SValue.SText] = Gen.alphaStr.map(SValue.SText)
  implicit val genSContractId: Gen[SValue.SContractId] = genContractIdV1.map(SValue.SContractId)
  implicit val genSParty: Gen[SValue.SParty] = genParty.map(SValue.SParty)

  implicit val genSRecord: Gen[SValue.SRecord] =
    for {
      id <- genIdentifier
      n <- Gen.choose(0, 10)
      fields <- Gen.listOfN(n, genName)
      values <- Gen.listOf(genSValue)
    } yield SValue.SRecord(id, ImmArray.from(fields), values.to(ArraySeq))

  implicit val genSVariant: Gen[SValue.SVariant] =
    for {
      id <- genIdentifier
      n <- Gen.choose(0, 10)
      constructor <- genName
      values <- genSValue
    } yield SValue.SVariant(id, constructor, n, values)

  implicit val genSEnum: Gen[SValue.SEnum] =
    for {
      id <- genIdentifier
      n <- Gen.choose(0, 10)
      constructor <- genName
    } yield SValue.SEnum(id, constructor, n)

  implicit val genSList: Gen[SValue.SList] = genFrontStack(genSValue).map(SValue.SList(_))

  implicit val genSOptional: Gen[SValue.SOptional] =
    Gen.option(genSValue).map(SValue.SOptional(_))

  implicit val genSMap: Gen[SValue.SMap] = for {
    isTextMap <- Gen.oneOf(true, false)
    keyGen = if (isTextMap) genSText else genSValue
    tree <- genTreeMap(keyGen, genSValue, SValue.SMap.`SMap Ordering`)
  } yield SValue.SMap(isTextMap, tree)

}

abstract class Sized[T] {

  // Footprint calculated with jol
  def shallowFootprint(x: T): Long

  // approximated footprint
  def approximateShallowFootprint(x: T): Long

  // sum of recursive footprint
  def approximateFootprint(x: T): Long

  def bloat(x: T) = () // try to expand the footprint of the object (e.g. filling internal caches)
}

object Sized {

  val precise: Boolean = true

  def pad(rawSize: Long) =
    if (precise)
      ((rawSize + 7) / 8) * 8
    else
      7 + rawSize

  def footprint(x: Any): Long = GraphLayout.parseInstance(x).totalSize()

  class SizedConstant[T](val const: T, approximateShallowFootPrint_ : Long) extends Sized[T] {

    final override def shallowFootprint(x: T): Long = Sized.footprint(x)

    final override def approximateFootprint(x: T): Long = approximateShallowFootprint(x)

    final override def approximateShallowFootprint(x: T): Long = approximateShallowFootPrint_
  }

  class SizedFixSizeAtom[T](approximateFootprint_ : Long) extends Sized[T] {
    final override def approximateFootprint(x: T): Long = approximateShallowFootprint(x)

    final override def shallowFootprint(x: T): Long = Sized.footprint(x)

    final override def approximateShallowFootprint(x: T): Long = approximateFootprint_
  }

  abstract class SizedVariableLengthAtom[T] extends Sized[T] {
    def length(x: T): Int

    def approximateFootprint(n: Int): Long

    final override def approximateFootprint(x: T): Long =
      approximateShallowFootprint(x: T)

    final override def shallowFootprint(x: T): Long = Sized.footprint(x)

    final override def approximateShallowFootprint(x: T): Long = approximateFootprint(length(x))
  }

  class SizedWrapper1[T, A](
      val wrap: A => T,
      val unwrap: T => A,
      val approximateShallowFootprint_ : Long,
  )(implicit
      a: Sized[A]
  ) extends Sized[T] {

    override final def shallowFootprint(x: T): Long =
      Sized.footprint(x) - Sized.footprint(unwrap(x))

    final override def approximateFootprint(x: T): Long =
      approximateShallowFootprint(x) +
        a.approximateFootprint(unwrap(x))

    final override def approximateShallowFootprint(x: T): Long = approximateShallowFootprint_
  }

  class SizedWrapper2[T, A, B](
      val wrap: (A, B) => T,
      val unwrap: T => (A, B),
      approximateShallowFootprint_ : Long,
  )(implicit a: Sized[A], b: Sized[B])
      extends Sized[T] {

    final override def approximateFootprint(x: T): Long = {
      val (as, bs) = unwrap(x)
      approximateShallowFootprint(x) + a.approximateFootprint(as) + b.approximateFootprint(bs)
    }

    final override def shallowFootprint(x: T): Long = {
      val (as, bs) = unwrap(x)
      Sized.footprint(x) - Sized.footprint(as) - Sized.footprint(bs)
    }

    final override def approximateShallowFootprint(x: T): Long = approximateShallowFootprint_
  }

  class SizedWrapper3[T, A, B, C](
      val wrap: (A, B, C) => T,
      val unwrap: T => (A, B, C),
      approximateShallowFootprint_ : Long,
  )(implicit a: Sized[A], b: Sized[B], c: Sized[C])
      extends Sized[T] {

    final override def approximateFootprint(x: T): Long = {
      val (as, bs, cs) = unwrap(x)
      this.approximateShallowFootprint(x) + a.approximateFootprint(as) + b.approximateFootprint(
        bs
      ) + c.approximateFootprint(cs)
    }

    final override def shallowFootprint(x: T): Long = {
      val (as, bs, cs) = unwrap(x)
      Sized.footprint(x) - Sized.footprint(as) - Sized.footprint(bs) - Sized.footprint(cs)
    }

    final override def approximateShallowFootprint(x: T): Long = approximateShallowFootprint_
  }

  abstract class SizedContainer1[T[_], A](implicit a: Sized[A]) extends Sized[T[A]] {

    def elements(x: T[A]): Iterator[A]

    def size(x: T[A]): Int = elements(x).size

    def fromList(as: List[A]): T[A]

    final override def shallowFootprint(x: T[A]): Long =
      elements(x).foldLeft(Sized.footprint(x))((acc, x) => acc - Sized.footprint(x))

    final override def approximateShallowFootprint(x: T[A]): Long = approximateShallowFootprint(
      size(x)
    )

    final override def approximateFootprint(x: T[A]): Long =
      elements(x).foldLeft(this.approximateShallowFootprint(size(x)))((acc, x) =>
        acc + a.approximateFootprint(x)
      )

    def approximateShallowFootprint(n: Int): Long

  }

  abstract class SizedContainer2[T[_, _], A, B](implicit a: Sized[A], b: Sized[B])
      extends Sized[T[A, B]] {

    def elements(x: T[A, B]): (Iterator[(A, B)])

    def size(x: T[A, B]): Int = elements(x).size

    def fromList(a: List[(A, B)]): T[A, B]

    def approximateShallowFootprint(n: Int): Long

    final override def approximateFootprint(x: T[A, B]): Long =
      this.approximateShallowFootprint(size(x)) +
        elements(x).map { case (x, y) =>
          a.approximateFootprint(x) + b.approximateFootprint(y)
        }.sum +
        corrections.map(Sized.footprint).sum

    final override def shallowFootprint(x: T[A, B]): Long = {
      Sized.footprint(x) -
        (corrections ++ elements(x).flatMap { case (x, y) => List[Any](x, y) })
          .map(Sized.footprint)
          .sum
    }

    final override def approximateShallowFootprint(x: T[A, B]): Long = approximateShallowFootprint(
      size(x)
    )

    def corrections: List[Any] = List.empty
  }

  implicit def sizedList[A](implicit a: Sized[A]): SizedContainer1[List, A] =
    new SizedContainer1[List, A] {
      override def elements(x: List[A]): Iterator[A] = x.iterator

      override def fromList(as: List[A]): List[A] = as

      override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.ListSize.calculate(n) +
          CostModelV0.OBJECT_HEADER_BYTES // CostModel0 does not charge for Nil but jol counts it.
    }

  implicit def SizedArray[A: ClassTag](implicit a: Sized[A]): SizedContainer1[Array, A] =
    new SizedContainer1[Array, A] {

      final override def elements(x: Array[A]): Iterator[A] = x.iterator

      final override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.AnyRefArraySize.calculate(n)

      final override def fromList(as: List[A]): Array[A] = as.toArray
    }

  implicit def SizedArraySeq[A <: AnyRef: ClassTag](implicit
      a: Sized[A]
  ): SizedContainer1[ArraySeq, A] = new SizedContainer1[ArraySeq, A]() {
    override def elements(x: ArraySeq[A]): Iterator[A] = x.iterator

    override def fromList(as: List[A]): ArraySeq[A] = ArraySeq.from(as)

    override def approximateShallowFootprint(n: Int): Long =
      CostModelV0.AnyRefArraySeqSize.calculate(n)
  }

  implicit def SizedOption[A](implicit a: Sized[A]): SizedContainer1[Option, A] =
    new SizedContainer1[scala.Option, A] {

      final override def elements(x: Option[A]): Iterator[A] = x.iterator

      final override def fromList(a: List[A]): Option[A] = a.headOption

      final override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.OptionSize
    }

  implicit def SizeTreeMap[K, V](implicit
      k: Sized[K],
      v: Sized[V],
      ordering: Ordering[K],
  ): SizedContainer2[TreeMap, K, V] = new SizedContainer2[TreeMap, K, V] {
    final override def elements(x: TreeMap[K, V]): Iterator[(K, V)] = x.iterator

    final override def approximateShallowFootprint(n: Int): Long = 32L + n * 56L

    final override def fromList(a: List[(K, V)]): TreeMap[K, V] =
      a.to(TreeMap)

    override def corrections: List[Any] = List(ordering)
  }

  implicit val SizedByteArray: SizedVariableLengthAtom[Array[Byte]] =
    new SizedVariableLengthAtom[Array[Byte]] {
      override def length(x: Array[Byte]): Int = x.length

      override def approximateFootprint(n: Int): Long =
        CostModelV0.ByteArraySize.calculate(n)

    }

  implicit lazy val SizedString: SizedVariableLengthAtom[String] =
    new SizedVariableLengthAtom[java.lang.String] {

      final override def length(x: String): Int = x.length

      final override def approximateFootprint(n: Int): Long =
        CostModelV0.StringSize.calculate(n)
    }

  implicit val SizedBytesString: SizedVariableLengthAtom[protobuf.ByteString] =
    new SizedVariableLengthAtom[protobuf.ByteString] {
      override def length(x: protobuf.ByteString): Int = x.size()

      override def approximateFootprint(n: Int): Long =
        CostModelV0.ByteStringSize.calculate(n)
    }

  implicit def SizedImmArray[A](implicit a: Sized[A]): SizedContainer1[ImmArray, A] =
    new SizedContainer1[ImmArray, A] {

      override def elements(x: ImmArray[A]): Iterator[A] = x.iterator

      override def approximateShallowFootprint(n: Int): Long = CostModelV0.ImmArraySize.calculate(n)

      override def fromList(a: List[A]): ImmArray[A] = a.to(ImmArray)
    }

  implicit def SizedFrontStack[A](implicit a: Sized[A]): SizedContainer1[FrontStack, A] =
    new SizedContainer1[FrontStack, A] {

      override def elements(x: FrontStack[A]): Iterator[A] = x.iterator

      override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.FrontStackSize.calculate(n)

      override def fromList(as: List[A]): FrontStack[A] =
        as.foldLeft(FrontStack.empty[A])(_.+:(_))
    }

  implicit val SizedBytes: SizedVariableLengthAtom[Bytes] =
    new SizedVariableLengthAtom[Bytes] {
      override def length(x: Bytes): Int = x.length

      override def approximateFootprint(n: Int): Long =
        CostModelV0.BytesSize.calculate(n)
    }

  implicit val SizedHash: SizedFixSizeAtom[crypto.Hash] = new SizedFixSizeAtom[crypto.Hash](
    CostModelV0.HashSize
  )

  implicit lazy val SizedNumeric: Sized[Numeric] =
    new SizedFixSizeAtom[Numeric](CostModelV0.NumericSize) {
      final override def bloat(x: Numeric) = {
        val _ = x.toString
      }
    }

  implicit lazy val SizedDate: Sized[Time.Date] =
    new SizedFixSizeAtom[Time.Date](CostModelV0.DateSize)

  implicit lazy val SizedTimestamp: Sized[Time.Timestamp] =
    new SizedFixSizeAtom[Time.Timestamp](CostModelV0.TimestampSize)

  implicit lazy val SizedSUnit: SizedConstant[SUnit.type] =
    new SizedConstant(
      SUnit,
      CostModelV0.SUnitSize +
        CostModelV0.OBJECT_HEADER_BYTES, // CostModel0 does not charge for SUnit, but jol counts it.
    )

  implicit object SizedSBool extends SizedFixSizeAtom[SBool](CostModelV0.SBoolSize)

  implicit lazy val SizedSInt64: Sized[SInt64] =
    new SizedFixSizeAtom[SInt64](CostModelV0.SInt64Size)

  implicit lazy val SizedSNumeric: SizedWrapper1[SNumeric, Numeric] =
    new SizedWrapper1[SNumeric, Numeric](SNumeric.apply, _.value, CostModelV0.SNumericWrapperSize)

  implicit lazy val SizedSText: SizedWrapper1[SText, String] =
    new SizedWrapper1[SText, String](SText.apply, _.value, CostModelV0.STextWrapperSize)

  implicit lazy val SizedSTimestamp: SizedWrapper1[STimestamp, Time.Timestamp] =
    new SizedWrapper1[STimestamp, Time.Timestamp](
      STimestamp.apply,
      _.value,
      CostModelV0.STimestampWrapperSize,
    )

  implicit lazy val SizedSDate: SizedWrapper1[SDate, Time.Date] =
    new SizedWrapper1[SDate, Time.Date](SDate.apply, _.value, CostModelV0.SDateWrapperSize)

  implicit lazy val SizedSValue: Sized[SValue] = new Sized[SValue] {

    override def approximateFootprint(x: SValue): Long =
      x match {
        case x: SInt64 => SizedSInt64.approximateFootprint(x)
        case x: SNumeric => SizedSNumeric.approximateFootprint(x)
        case x: SText => SizedSText.approximateFootprint(x)
        case x: STimestamp => SizedSTimestamp.approximateFootprint(x)
        case x: SBool => SizedSBool.approximateFootprint(x)
        case x: SDate => SizedSDate.approximateFootprint(x)
        case _ => throw new IllegalArgumentException(s"not implemented")
      }

    override def shallowFootprint(x: SValue): Long = footprint(x)

    override def approximateShallowFootprint(x: SValue): Long = approximateFootprint(x)
  }
}
