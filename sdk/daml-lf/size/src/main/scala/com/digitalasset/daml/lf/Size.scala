// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.google.protobuf
import org.openjdk.jol.info._

import scala.annotation.nowarn
import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag

abstract class Sized[T] {

  // Footprint calculated with jol
  def shallowFootprint(x: T): Long

  // approximated footprint
  def approximateShallowFootprint(x: T): Long

  // sum of recursive footprint
  def approximateFootprint(x: T): Long

  def bloat(x: T) = () // try to expand the footprint of the object (e.g. filling internal caches)
}

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
    val approximateShallowFootprint_ : Long = Sized.OBJECT_HEADER_S + Sized.REFERENCE_S,
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
    approximateShallowFootprint_ : Long = Sized.OBJECT_HEADER_S + 2 * Sized.REFERENCE_S,
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
    approximateShallowFootprint_ : Long = Sized.OBJECT_HEADER_S + 3 * Sized.REFERENCE_S,
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

  def fromList(a: List[A]): T[A]

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
      elements(x).map { case (x, y) => a.approximateFootprint(x) + b.approximateFootprint(y) }.sum +
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

@nowarn("msg=implicit numeric widening")
object Sized {

  val OBJECT_HEADER_S = 16L
  val REFERENCE_S = 8L
  val LONG_S = 8L

  val precise: Boolean = true

  def pad(rawSize: Long) =
    if (precise)
      ((rawSize + 7) / 8) * 8
    else
      7 + rawSize

  def footprint(x: Any): Long = GraphLayout.parseInstance(x).totalSize()

  implicit def sizesList[A](implicit a: Sized[A]): SizedContainer1[List, A] =
    new SizedContainer1[List, A] {
      override def elements(x: List[A]): Iterator[A] = x.iterator

      override def fromList(as: List[A]): List[A] = as

      override def approximateShallowFootprint(n: Int): Long = {
        OBJECT_HEADER_S + // Nil
          n * (OBJECT_HEADER_S + REFERENCE_S + REFERENCE_S) // n Cons
      }
    }

  implicit def SizedArray[A: ClassTag](implicit a: Sized[A]): SizedContainer1[Array, A] =
    new SizedContainer1[Array, A] {

      final override def elements(x: Array[A]): Iterator[A] = x.iterator

      final override def approximateShallowFootprint(n: Int): Long =
        OBJECT_HEADER_S +
          LONG_S + // array size
          n * REFERENCE_S

      final override def fromList(as: List[A]): Array[A] = as.toArray
    }

  implicit def SizedOption[A](implicit a: Sized[A]): SizedContainer1[Option, A] =
    new SizedContainer1[scala.Option, A] {

      final override def elements(x: Option[A]): Iterator[A] = x.iterator

      final override def fromList(a: List[A]): Option[A] = a.headOption

      final override def approximateShallowFootprint(n: Int): Long =
        n match {
          case 0 => OBJECT_HEADER_S // None
          case 1 => OBJECT_HEADER_S + REFERENCE_S // Some
          case _ =>
            throw new IllegalArgumentException(s"Option can only have 0 or 1 elements, got $n")
        }
    }

  implicit def SizeTreeMap[K, V](implicit
      k: Sized[K],
      v: Sized[V],
      ordering: Ordering[K],
  ): SizedContainer2[TreeMap, K, V] = new SizedContainer2[TreeMap, K, V] {
    final override def elements(x: TreeMap[K, V]): Iterator[(K, V)] = x.iterator

    final override def approximateShallowFootprint(n: Int): Long = 32 + n * 56

    final override def fromList(a: List[(K, V)]): TreeMap[K, V] =
      a.to(TreeMap)

    override def corrections: List[Any] = List(ordering)
  }

  implicit val SizedByteArray: SizedVariableLengthAtom[Array[Byte]] =
    new SizedVariableLengthAtom[Array[Byte]] {
      override def length(x: Array[Byte]): Int = x.length

      override def approximateFootprint(n: Int): Long =
        OBJECT_HEADER_S + //
          LONG_S + // array size
          pad(n)

    }

  implicit lazy val SizedString: SizedVariableLengthAtom[String] =
    new SizedVariableLengthAtom[java.lang.String] {

      final override def length(x: String): Int = x.length

      final override def approximateFootprint(n: Int): Long = // 32 + 24 + pad(n)
        OBJECT_HEADER_S +
          LONG_S + // hashCode
          REFERENCE_S + // Array[Byte] ref
          SizedByteArray.approximateFootprint(n) // Array[Byte]
    }

  implicit val SizedBytesString: SizedVariableLengthAtom[protobuf.ByteString] =
    new SizedVariableLengthAtom[protobuf.ByteString] {
      override def length(x: protobuf.ByteString): Int = x.size()
      override def approximateFootprint(n: Int): Long = 56 + pad(n)
    }

}
