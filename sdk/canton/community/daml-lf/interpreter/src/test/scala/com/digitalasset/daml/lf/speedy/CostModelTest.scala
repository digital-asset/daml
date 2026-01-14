// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml
package lf

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.speedy.CostModel.CostModelV0
import com.digitalasset.daml.lf.speedy.SValue
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
      verbose: Boolean = false,
      minSucc: PosInt = 10,
  )(implicit sized: Sized[T]): Unit =
    s"approximate real footprint of $name" in {
      forAll(sized.gen(0), minSuccessful(minSucc)) { x: T =>
        sized.bloat(x)

        val shallowFootprint = sized.shallowFootprint(x)
        if (shallowFootprint <= 0)
          assert(sized.shallowFootprint(x) > 0)
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

        if (shallowFootprint > approximativeShallowSize) {
          if (strict)
            shallowFootprint shouldBe approximativeShallowSize

          shallowFootprint should be <= approximativeShallowSize
        }

        val footprint = Sized.footprint(x)
        val approximateFootprint = sized.approximateFootprint(x)
        if (footprint > approximateFootprint)
          footprint should be <= approximateFootprint
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

    test[Option[String]]("Option[String]", strict = true, minSucc = 100)
    test[List[String]]("List[String]", strict = true, minSucc = 100)
    test[Array[String]]("Array[String]", strict = true)

    /* LF data structures */

    test[Bytes]("Bytes", strict = true)
    test[crypto.Hash]("Hash", strict = false)
    test[Numeric]("Numeric", strict = false, minSucc = 100)
    test[Time.Date]("Date", strict = true)
    test[Time.Timestamp]("Timestamp", strict = true)

    test[ImmArray[MyLong]]("ImmArray[MyInt]", strict = true)
    test[FrontStack[MyLong]]("FrontStack[MyInt]", strict = true)

    test[ImmArray[String]]("ImmArray[String]", strict = true)
    test[FrontStack[String]]("FrontStack[String]", strict = true, minSucc = 100)

    /* Speedy atoms */

    test[SValue.SUnit.type]("SUnit", strict = true, minSucc = 1)
    test[SValue.SBool]("SBool", strict = true, minSucc = 2)(Sized.SizedSBool)
    test[SValue.SInt64]("SInt64", strict = true)
    test[SValue.SDate](name = "SDate", strict = true)
    test[SValue.STimestamp](name = "STimestamp", strict = true)
    test[SValue.SText](name = "SText", strict = true)
    test[SValue.SNumeric](name = "SNumeric", strict = true)
    test[SValue.SEnum](name = "SEnum", strict = true)

    /* Speedy data structure */

    test[SValue.SOptional]("SOptional", strict = false, minSucc = 100)
    test[SValue.SList]("SList", strict = false, minSucc = 100)
    test[SValue.SRecord]("SRecord", minSucc = 100)
    test[SValue.SVariant]("SVariant", strict = true, minSucc = 100)

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
    implicit val sized: Sized[MyLong] =
      new SizedFixSizeAtom[MyLong](Gen.long.map(new MyLong(_)), 24)
  }

  implicit def buildableArray[T: ClassTag]: Buildable[T, Array[T]] =
    new Buildable[T, Array[T]] {
      def builder = Array.newBuilder[T]
    }

  implicit def genByte: Gen[Byte] = Gen.chooseNum(Byte.MinValue, Byte.MaxValue)

  def genByteArrayN(n: Int) = Gen.containerOfN(n, genByte)

  def genByteArray: Gen[Array[Byte]] =
    Gen.sized(s => Gen.choose(0, s max 0)).flatMap(genByteArrayN)

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

  def genNonEmptyAsciiString = for {
    m <- Gen.size
    chars <- Gen.listOfN(m + 1, Gen.asciiChar)
  } yield chars.mkString

  def genParty: Gen[Ref.Party] = genNonEmptyAsciiString.map(Ref.Party.assertFromString)

  def genName: Gen[Ref.Name] = genNonEmptyAsciiString.map(Ref.Name.assertFromString)

  implicit def genDottedName: Gen[Ref.DottedName] = ValueGenerators.dottedNameGen

  implicit def genQualifiedName: Gen[Ref.QualifiedName] = genIdentifier.map(_.qualifiedName)

  implicit def genIdentifier: Gen[Ref.Identifier] = ValueGenerators.idGen

  def genSize(n: Int) = Gen.choose(0, 1 << (n << 2))

}

abstract class Sized[T] {

  def gen(n: Int = 3): Gen[T]

  // Footprint calculated with jol
  def shallowFootprint(x: T): Long

  // approximated footprint
  def approximateShallowFootprint(x: T): Long

  // sum of recursive footprint
  def approximateFootprint(x: T): Long

  def bloat(x: T) = () // try to expand the footprint of the object (e.g. filling internal caches)
}

object Sized {

  import CostModelTest._

  def footprint(x: Any): Long = {
    val f1 = Footprint.footprint(x)
    val f2 = GraphLayout.parseInstance(x).totalSize()
    assert(f2 <= f1)
    f1
  }

  class SizedConstant[T](val const: T, approximateShallowFootPrint_ : Long) extends Sized[T] {

    final override def gen(n: Int): Gen[T] = Gen.const(const)

    final override def shallowFootprint(x: T): Long = Sized.footprint(x)

    final override def approximateFootprint(x: T): Long = approximateShallowFootprint(x)

    final override def approximateShallowFootprint(x: T): Long = approximateShallowFootPrint_
  }

  class SizedFixSizeAtom[T](gen0: Gen[T], approximateFootprint_ : Long) extends Sized[T] {
    final override def approximateFootprint(x: T): Long = approximateShallowFootprint(x)

    final override def shallowFootprint(x: T): Long = Sized.footprint(x)

    final override def approximateShallowFootprint(x: T): Long = approximateFootprint_

    override def gen(n: Int): Gen[T] = gen0
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

    def gen(n: Int): Gen[T] = for {
      x <- a.gen(n)
    } yield wrap(x)

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

    def gen(n: Int): Gen[T] = for {
      x <- a.gen(n)
      y <- b.gen(n)
    } yield wrap(x, y)

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

    def gen(n: Int): Gen[T] = for {
      x <- a.gen(n)
      y <- b.gen(n)
      z <- c.gen(n)
    } yield wrap(x, y, z)

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

  abstract class SizedContainer1[T, A](implicit a: Sized[A]) extends Sized[T] {

    def gen(n: Int): Gen[T] =
      for {
        m <- genSize(n)
        xs <- Gen.listOfN(m, a.gen(n))
      } yield fromList(xs)

    def elements(x: T): Iterator[A]

    def size(x: T): Int = elements(x).size

    def fromList(as: List[A]): T

    final override def shallowFootprint(x: T): Long =
      Sized.footprint(x) - elements(x).map(Sized.footprint).sum

    final override def approximateShallowFootprint(x: T): Long =
      approximateShallowFootprint(size(x))

    final override def approximateFootprint(x: T): Long =
      approximateShallowFootprint(x) +
        elements(x).map(a.approximateFootprint).sum

    def approximateShallowFootprint(n: Int): Long

  }

  abstract class SizedContainer2[T, A, B](implicit a: Sized[A], b: Sized[B]) extends Sized[T] {

    def genPair(n: Int): Gen[(A, B)] = for {
      x <- a.gen(n)
      y <- b.gen(n)
    } yield (x, y)

    def gen(n: Int): Gen[T] =
      for {
        xs <- Gen.listOf(genPair(n))
      } yield fromList(xs)

    def elements(x: T): (Iterator[(A, B)])

    def size(x: T): Int = elements(x).size

    def fromList(a: List[(A, B)]): T

    def approximateShallowFootprint(n: Int): Long

    final override def approximateFootprint(x: T): Long =
      this.approximateShallowFootprint(size(x)) +
        elements(x).map { case (x, y) =>
          a.approximateFootprint(x) + b.approximateFootprint(y)
        }.sum +
        corrections.map(Sized.footprint).sum

    final override def shallowFootprint(x: T): Long = {
      Sized.footprint(x) -
        (corrections ++ elements(x).flatMap { case (x, y) => List[Any](x, y) })
          .map(Sized.footprint)
          .sum
    }

    final override def approximateShallowFootprint(x: T): Long =
      approximateShallowFootprint(size(x))

    def corrections: List[Any] = List.empty
  }

  class NoCostModel[A](gen0: Gen[A]) extends Sized[A] {

    override def shallowFootprint(x: A): Long = Sized.footprint(x)

    override def approximateShallowFootprint(x: A): Long = shallowFootprint(x)

    override def approximateFootprint(x: A): Long = shallowFootprint(x)

    override def gen(n: Int): Gen[A] = gen0
  }

  implicit val IdentifierNoCostModel: Sized[Ref.Identifier] = new NoCostModel(ValueGenerators.idGen)
  implicit val NameNoCostModel: Sized[Ref.Name] = new NoCostModel(ValueGenerators.nameGen)

  implicit def sizedList[A](implicit a: Sized[A]): SizedContainer1[List[A], A] =
    new SizedContainer1[List[A], A] {
      override def elements(x: List[A]): Iterator[A] = x.iterator

      override def fromList(as: List[A]): List[A] = as

      override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.ListSize.calculate(n) +
          CostModelV0.OBJECT_HEADER_BYTES // CostModel0 does not charge for Nil but jol counts it.
    }

  implicit def SizedArray[A: ClassTag](implicit a: Sized[A]): SizedContainer1[Array[A], A] =
    new SizedContainer1[Array[A], A] {

      final override def elements(x: Array[A]): Iterator[A] = x.iterator

      final override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.AnyRefArraySize.calculate(n)

      final override def fromList(as: List[A]): Array[A] = as.toArray
    }

  implicit def SizedArraySeq[A <: AnyRef: ClassTag](implicit
      a: Sized[A]
  ): SizedContainer1[ArraySeq[A], A] = new SizedContainer1[ArraySeq[A], A]() {
    override def elements(x: ArraySeq[A]): Iterator[A] = x.iterator

    override def fromList(as: List[A]): ArraySeq[A] = ArraySeq.from(as)

    override def approximateShallowFootprint(n: Int): Long =
      CostModelV0.AnyRefArraySeqSize.calculate(n)
  }

  implicit def SizedOption[A](implicit a: Sized[A]): SizedContainer1[Option[A], A] =
    new SizedContainer1[scala.Option[A], A] {

      final override def elements(x: Option[A]): Iterator[A] = x.iterator

      final override def fromList(a: List[A]): Option[A] = a.headOption

      final override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.OptionSize
    }

  implicit def SizeTreeMap[K, V](implicit
      k: Sized[K],
      v: Sized[V],
      ordering: Ordering[K],
  ): SizedContainer2[TreeMap[K, V], K, V] = new SizedContainer2[TreeMap[K, V], K, V] {
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

      override def gen(n: Int): Gen[Array[Byte]] = genByteArray
    }

  implicit lazy val SizedString: SizedVariableLengthAtom[String] =
    new SizedVariableLengthAtom[java.lang.String] {

      final override def length(x: String): Int = x.length

      final override def approximateFootprint(n: Int): Long =
        CostModelV0.StringSize.calculate(n)

      override def gen(n: Int): Gen[String] =
        for {
          n <- Gen.size
          chars <- Gen.listOfN(n, Gen.alphaChar)
        } yield new String(chars.toArray)
    }

  implicit val SizedBytesString: SizedVariableLengthAtom[protobuf.ByteString] =
    new SizedVariableLengthAtom[protobuf.ByteString] {
      override def length(x: protobuf.ByteString): Int = x.size()

      override def approximateFootprint(n: Int): Long =
        CostModelV0.ByteStringSize.calculate(n)

      override def gen(n: Int): Gen[protobuf.ByteString] = for {
        m <- genSize(n)
        bytes <- genByteArrayN(m)
      } yield protobuf.ByteString.copyFrom(bytes)
    }

  implicit def SizedImmArray[A](implicit a: Sized[A]): SizedContainer1[ImmArray[A], A] =
    new SizedContainer1[ImmArray[A], A] {

      override def elements(x: ImmArray[A]): Iterator[A] = x.iterator

      override def approximateShallowFootprint(n: Int): Long = CostModelV0.ImmArraySize.calculate(n)

      override def fromList(a: List[A]): ImmArray[A] = a.to(ImmArray)
    }

  implicit def SizedFrontStack[A](implicit a: Sized[A]): SizedContainer1[FrontStack[A], A] =
    new SizedContainer1[FrontStack[A], A] {

      override def elements(x: FrontStack[A]): Iterator[A] = x.iterator

      override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.FrontStackSize.calculate(n)

      override def fromList(as: List[A]): FrontStack[A] = {
        // Worth case for building a Fronstack
        as.foldRight(FrontStack.empty[A])((x, acc) => ImmArray(x) ++: acc)
      }
    }

  implicit val SizedBytes: SizedVariableLengthAtom[Bytes] =
    new SizedVariableLengthAtom[Bytes] {
      override def length(x: Bytes): Int = x.length

      override def approximateFootprint(n: Int): Long =
        CostModelV0.BytesSize.calculate(n)

      override def gen(n: Int): Gen[Bytes] = genByteArray.map(Bytes.fromByteArray)
    }

  implicit val SizedHash: SizedFixSizeAtom[crypto.Hash] = new SizedFixSizeAtom[crypto.Hash](
    genByteArrayN(32).map(crypto.Hash.assertFromByteArray),
    CostModelV0.HashSize,
  )

  implicit lazy val SizedNumeric: Sized[Numeric] =
    new SizedFixSizeAtom[Numeric](
      ValueGenerators.unscaledNumGen,
      CostModelV0.NumericSize,
    ) {
      final override def bloat(x: Numeric) = {
        val _ = x.toString
      }
    }

  implicit lazy val SizedDate: Sized[Time.Date] =
    new SizedFixSizeAtom[Time.Date](ValueGenerators.dateGen, CostModelV0.DateSize)

  implicit lazy val SizedTimestamp: Sized[Time.Timestamp] =
    new SizedFixSizeAtom[Time.Timestamp](ValueGenerators.timestampGen, CostModelV0.TimestampSize)

  implicit lazy val SizedParty: Sized[Ref.Party] =
    new SizedVariableLengthAtom[Party] {
      override def length(x: Party): Int = x.length

      override def approximateFootprint(n: Int): Long =
        CostModelV0.AsciiStringSize.calculate(n)

      override def gen(n: Int): Gen[Party] =
        ValueGenerators.party
    }

  implicit lazy val SizedSUnit: SizedConstant[SValue.SUnit.type] =
    new SizedConstant(
      const = SValue.SUnit,
      approximateShallowFootPrint_ = CostModelV0.SUnitSize +
        CostModelV0.OBJECT_HEADER_BYTES, // CostModel0 does not charge for SUnit, but jol counts it.
    )

  implicit object SizedSBool
      extends SizedFixSizeAtom[SValue.SBool](
        Gen.oneOf(SValue.SValue.True, SValue.SValue.False),
        CostModelV0.SBoolSize,
      )

  implicit lazy val SizedSInt64: Sized[SValue.SInt64] =
    new SizedFixSizeAtom[SValue.SInt64](
      Gen.long.map(SValue.SInt64(_)),
      CostModelV0.SInt64Size,
    )

  implicit lazy val SizedSNumeric: SizedWrapper1[SValue.SNumeric, Numeric] =
    new SizedWrapper1[SValue.SNumeric, Numeric](
      SValue.SNumeric.apply,
      _.value,
      CostModelV0.SNumericWrapperSize,
    )

  implicit lazy val SizedSText: SizedWrapper1[SValue.SText, String] =
    new SizedWrapper1[SValue.SText, String](
      SValue.SText.apply,
      _.value,
      CostModelV0.STextWrapperSize,
    )

  implicit lazy val SizedSParty: SizedWrapper1[SValue.SParty, Ref.Party] =
    new SizedWrapper1[SValue.SParty, Ref.Party](
      SValue.SParty.apply,
      _.value,
      CostModelV0.SPartyWrapperSize,
    )

  implicit lazy val SizedSTimestamp: SizedWrapper1[SValue.STimestamp, Time.Timestamp] =
    new SizedWrapper1[SValue.STimestamp, Time.Timestamp](
      SValue.STimestamp.apply,
      _.value,
      CostModelV0.STimestampWrapperSize,
    )

  implicit lazy val SizedSDate: SizedWrapper1[SValue.SDate, Time.Date] =
    new SizedWrapper1[SValue.SDate, Time.Date](
      SValue.SDate.apply,
      _.value,
      CostModelV0.SDateWrapperSize,
    )

  implicit lazy val SizedSOptional: SizedContainer1[SValue.SOptional, SValue] =
    new SizedContainer1[SValue.SOptional, SValue] {
      override def elements(x: SValue.SOptional): Iterator[SValue] = x.value.iterator

      override def fromList(as: List[SValue]): SValue.SOptional =
        SValue.SOptional(as.headOption)

      override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.SOptionalSize
    }

  implicit lazy val SizedSList: SizedContainer1[SValue.SList, SValue] =
    new SizedContainer1[SValue.SList, SValue] {
      override def elements(x: SValue.SList): Iterator[SValue] = x.list.iterator

      override def fromList(as: List[SValue]): SValue.SList =
        SValue.SList(FrontStack.from(as))

      override def approximateShallowFootprint(n: Int): Long =
        CostModelV0.SListSize.calculate(n)
    }

  implicit lazy val SizedSEnum: SizedWrapper2[SValue.SEnum, Ref.Identifier, Ref.Name] =
    new SizedWrapper2[
      SValue.SEnum,
      Ref.Identifier,
      Ref.Name,
    ](
      SValue.SEnum.apply(_, _, 0),
      x => (x.id, x.constructor),
      CostModelV0.SEnumSize,
    )

  implicit lazy val SizedSVariant
      : SizedWrapper3[SValue.SVariant, Ref.Identifier, Ref.Name, SValue] =
    new SizedWrapper3[SValue.SVariant, Ref.Identifier, Ref.Name, SValue](
      SValue.SVariant.apply(_, _, 0, _),
      x => (x.id, x.variant, x.value),
      CostModelV0.SVariantWrapperSize,
    )

  implicit lazy val SizedSRecord: Sized[SValue.SRecord] = new Sized[SValue.SRecord] {

    override def gen(n: Int): Gen[SValue.SRecord] = for {
      // id <- ValueGenerators.idGen
      id <- Gen.const(Ref.Identifier.assertFromString("p:M:T"))
      m <- Gen.choose(0, 128)
      // fields <- Gen.listOfN(m, ValueGenerators.nameGen)
      fields = (0 until m).map(i => Ref.Name.assertFromString(s"f$i"))
      values <- Gen.listOfN(m, SizedSValue.gen(n))
    } yield SValue.SRecord(id, fields.to(ImmArray), values.to(ArraySeq))

    override def shallowFootprint(x: SValue.SRecord): Long = {
      Sized.footprint(x) -
        Sized.footprint(x.id) - // we do not charge for the identifier
        Sized.footprint(x.fields) - // we do not charged for the field names
        x.values.view.map(Sized.footprint).sum

    }

    override def approximateShallowFootprint(x: SValue.SRecord): Long =
      CostModelV0.SRecordWrapperSize.calculate(x.values.size)

    override def approximateFootprint(x: SValue.SRecord): Long =
      approximateShallowFootprint(x) +
        Sized.footprint(x.id) +
        Sized.footprint(x.fields) +
        x.values.view.map(SizedSValue.approximateFootprint).sum
  }

  implicit lazy val SizedSStruct: Sized[SValue.SStruct] = new Sized[SValue.SStruct] {
    override def gen(n: Int): Gen[SValue.SStruct] = for {
      m <- Gen.choose(0, 128)
      fields <- Gen.listOfN(m, ValueGenerators.nameGen)
      values <- Gen.listOfN(m, SizedSValue.gen(n))
    } yield SValue.SStruct(data.Struct.assertFromNameSeq(fields), values.to(ArraySeq))

    override def shallowFootprint(x: SValue.SStruct): Long =
      Sized.footprint(x) -
        Sized.footprint(x.fieldNames) - // we do not charged for the field names
        x.values.view.map(Sized.footprint).sum

    override def approximateShallowFootprint(x: SValue.SStruct): Long =
      shallowFootprint(x) + approximateFootprint(x)

    override def approximateFootprint(x: SValue.SStruct): Long =
      CostModelV0.SStructWrapperSize.calculate(x.values.size) +
        Sized.footprint(x.fieldNames) +
        x.values.view.map(SizedSValue.approximateFootprint).sum
  }

  implicit lazy val SizedSValue: Sized[SValue] = new Sized[SValue] {

    override def approximateFootprint(x: SValue): Long =
      x match {
        case SValue.SUnit => SizedSUnit.approximateFootprint(SValue.SUnit)
        case x: SValue.SBool => SizedSBool.approximateFootprint(x)
        case x: SValue.SInt64 => SizedSInt64.approximateFootprint(x)
        case x: SValue.SNumeric => SizedSNumeric.approximateFootprint(x)
        case x: SValue.SText => SizedSText.approximateFootprint(x)
        case x: SValue.STimestamp => SizedSTimestamp.approximateFootprint(x)
        case x: SValue.SDate => SizedSDate.approximateFootprint(x)
        case x: SValue.SEnum => SizedSEnum.approximateFootprint(x)
        case x: SValue.SVariant => SizedSVariant.approximateFootprint(x)
        case x: SValue.SRecord => SizedSRecord.approximateFootprint(x)
        case x: SValue.SOptional => SizedSOptional.approximateFootprint(x)
        case x: SValue.SList => SizedSList.approximateFootprint(x)
        case x: SValue.SParty => SizedSParty.approximateFootprint(x)
        case x: SValue.SStruct => SizedSStruct.approximateFootprint(x)
        case SValue.SToken | _: SValue.STypeRep | _: SValue.SAny | _: SValue.SMap |
            _: SValue.SBigNumeric | _: SValue.SPAP | _: SValue.SContractId =>
          throw new IllegalArgumentException(s"not implemented")
      }

    override def shallowFootprint(x: SValue): Long = footprint(x)

    override def approximateShallowFootprint(x: SValue): Long = approximateFootprint(x)

    override def gen(n: Int): Gen[SValue] = {
      val base = Gen.oneOf(
        SizedSUnit.gen(n),
        SizedSBool.gen(n),
        SizedSInt64.gen(n),
        SizedSNumeric.gen(n),
        SizedSText.gen(n).filter(_.value.nonEmpty),
        SizedSDate.gen(n),
        SizedSTimestamp.gen(n),
        SizedSEnum.gen(n),
        SizedSParty.gen(n),
      )
      val recursive = Gen.oneOf(
        SizedSVariant.gen(n - 1),
        SizedSRecord.gen(n - 1),
        SizedSList.gen(n - 1),
        SizedSOptional.gen(n - 1),
        SizedSStruct.gen(n - 1),
      )

      Gen.frequency(
        5 -> base,
        1 * n.sign * 0 -> recursive,
      )
    }
  }
}

object Footprint {

  // Calculates the memory footprint of an object without accounting for shared references
  def footprint(x: Any): Long = {
    val vm = org.openjdk.jol.vm.VM.current()

    @scala.annotation.tailrec
    def loop(stack: List[(Any, IdentitySet)], acc0: Long): Long = {
      stack match {
        case (x, seen0) :: tail =>
          x match {
            case x: AnyRef =>
              val acc = acc0 + vm.sizeOf(x)

              val nexts = x match {
                case arr: Array[_] if !arr.getClass.getComponentType.isPrimitive =>
                  arr.view.collect { case a: AnyRef => a }
                case otherwise: AnyRef =>
                  val clazz = otherwise.getClass
                  clazz.getDeclaredFields.view
                    .filterNot(f => java.lang.reflect.Modifier.isStatic(f.getModifiers))
                    .filterNot(f => f.getType.isPrimitive)
                    .map { f =>
                      f.setAccessible(true)
                      f.get(otherwise).asInstanceOf[AnyRef]
                    }
                case _ =>
                  List.empty.view
              }
              val seen = seen0.add(x)
              loop(nexts.filterNot(_ == null).filterNot(seen.contains).map((_, seen)) ++: tail, acc)
            case _ =>
              loop(tail, acc0)
          }
        case Nil => acc0
      }
    }

    loop(List(x -> IdentitySet.Empty), 0L)
  }
}

final class IdentitySet private (map: Map[Int, List[AnyRef]]) {
  def contains(x: AnyRef): Boolean = {
    val hash = System.identityHashCode(x)
    map.get(hash).exists(_.exists(_ eq x))
  }

  def add(x: AnyRef): IdentitySet = {
    val hash = System.identityHashCode(x)
    val xs = map.getOrElse(hash, Nil)
    if (xs.exists(_ eq x)) this
    else new IdentitySet(map.updated(hash, x :: xs))
  }

  def remove(x: AnyRef): IdentitySet = {
    val hash = System.identityHashCode(x)
    val xs = map.getOrElse(hash, Nil).filterNot(_ eq x)
    if (xs.isEmpty) new IdentitySet(map - hash)
    else new IdentitySet(map.updated(hash, xs))
  }
}

object IdentitySet {
  val Empty = new IdentitySet(Map.empty)
}
