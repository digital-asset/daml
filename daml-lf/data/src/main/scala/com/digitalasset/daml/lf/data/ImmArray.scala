// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.{equalBy, orderBy, toIterableForScalazInstances}

import scalaz.syntax.applicative._
import scalaz.{Applicative, Equal, Foldable, Order, Traverse}

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.generic.{
  CanBuildFrom,
  GenericCompanion,
  GenericTraversableTemplate,
  IndexedSeqFactory
}
import scala.collection.immutable.IndexedSeq
import scala.collection.{IndexedSeqLike, IndexedSeqOptimized, mutable}
import scala.language.higherKinds
import scala.reflect.ClassTag

/** Simple immutable array. The intention is that all the operations have the "obvious"
  * operational behavior (like Vector in haskell).
  *
  * Functions that slice the `ImmArray` (including `head`, `tail`, etc) are all constant time --
  * they keep referring to the same underlying array.
  *
  * Note that we _very intentionally_ do _not_ make this an instance of any sorts of `Seq`, since
  * using `Seq` encourages patterns where the the performance of what you're doing is totally
  * unclear. Use `toSeq` if you want a `Seq`, and think about what that means.
  */
final class ImmArray[+A] private (
    private val start: Int,
    val length: Int,
    array: mutable.ArraySeq[A]
) {
  self =>

  def iterator: Iterator[A] = {
    var cursor = start

    new Iterator[A] {
      override def hasNext: Boolean = cursor < start + self.length

      override def next(): A = {
        val x = array(cursor)
        cursor += 1
        x
      }
    }
  }

  def reverseIterator: Iterator[A] = {
    var cursor = start + self.length - 1

    new Iterator[A] {
      override def hasNext: Boolean = cursor >= start

      override def next(): A = {
        val x = array(cursor)
        cursor -= 1
        x
      }
    }
  }

  /** O(1), crashes on out of bounds */
  def apply(idx: Int): A =
    if (idx >= length) {
      throw new IndexOutOfBoundsException("index out of bounds in ImmArray apply")
    } else {
      uncheckedGet(idx)
    }

  private def uncheckedGet(idx: Int): A = array(start + idx)

  /** O(n) */
  def copyToArray[B >: A](dst: Array[B], dstStart: Int, dstLen: Int): Unit = {
    for (i <- 0 until Math.max(length, dstLen)) {
      dst(dstStart + i) = uncheckedGet(i)
    }
  }

  /** O(n) */
  def copyToArray[B >: A](xs: Array[B]): Unit = {
    copyToArray(xs, 0, length)
  }

  /** O(1), crashes on empty list */
  def head: A = this(0)

  /** O(1), crashes on empty list */
  def last: A = this(length - 1)

  /** O(1), crashes on empty list */
  def tail: ImmArray[A] = {
    if (length < 1) {
      throw new RuntimeException("tail on empty ImmArray")
    } else {
      new ImmArray(start + 1, length - 1, array)
    }
  }

  /** O(1), crashes on empty list */
  def init: ImmArray[A] = {
    if (length < 1) {
      throw new RuntimeException("init on empty ImmArray")
    } else {
      new ImmArray(start, length - 1, array)
    }
  }

  /** O(1)
    *
    * Returns all the elements at indices `ix` where `from <= ix < until`.
    *
    * Crashes if:
    * * `from < 0 || from >= length`
    * * `until < 0 || until > length`
    *
    * Note that this crashing behavior is _not_ consistent with Seq's behavior of not crashing
    * but rather returning an empty array. If you want Seq's behavior,
    * use `relaxedSlice`.
    */
  def strictSlice(from: Int, until: Int): ImmArray[A] = {
    if (from < 0 || from >= length || until < 0 || until > length) {
      throw new IndexOutOfBoundsException(
        s"strictSlice arguments out of bounds for ImmArray. length: $length, from: $from, until: $until")
    }

    relaxedSlice(from, until)
  }

  /** O(1), like `strictSlice`, but does not crash on out of bounds errors -- follows the same semantics
    * as Seq's slice.
    */
  def relaxedSlice(from0: Int, until0: Int): ImmArray[A] = {
    val from = Math.max(Math.min(from0, length), 0)
    val until = Math.max(Math.min(until0, length), 0)

    val newLen = until - from
    if (newLen <= 0) {
      ImmArray.empty[A]
    } else {
      new ImmArray(start + from, newLen, array)
    }
  }

  /** O(n) */
  def map[B](f: A => B): ImmArray[B] = {
    val newArray: mutable.ArraySeq[B] = new mutable.ArraySeq(length)
    for (i <- indices) {
      newArray(i) = f(uncheckedGet(i))
    }
    ImmArray.unsafeFromArraySeq[B](newArray)
  }

  /** O(n) */
  def reverse: ImmArray[A] = {
    val newArray: mutable.ArraySeq[A] = new mutable.ArraySeq(length)
    for (i <- indices) {
      newArray(i) = array(start + length - (i + 1))
    }
    ImmArray.unsafeFromArraySeq[A](newArray)
  }

  /** O(n+m)
    *
    * If you find using this a lot, consider using another data structure (maybe FrontQueue or BackQueue),
    * since to append ImmArray we must copy both of them.
    */
  def slowAppend[B >: A](other: ImmArray[B]): ImmArray[B] = {
    val newArray: mutable.ArraySeq[B] = new mutable.ArraySeq(length + other.length)
    for (i <- indices) {
      newArray(i) = uncheckedGet(i)
    }
    for (i <- other.indices) {
      newArray(length + i) = other.uncheckedGet(i)
    }
    ImmArray.unsafeFromArraySeq(newArray)
  }

  /** O(n)
    *
    * If you find yourself using this a lot, consider using another data structure,
    * since to cons an ImmArray we must copy it.
    */
  def slowCons[B >: A](el: B): ImmArray[B] = {
    val newArray: mutable.ArraySeq[B] = new mutable.ArraySeq(length + 1)
    newArray(0) = el
    for (i <- indices) {
      newArray(i + 1) = uncheckedGet(i)
    }
    ImmArray.unsafeFromArraySeq(newArray)
  }

  /** O(n)
    *
    * If you find yourself using this a lot, consider using another data structure,
    * since to snoc an ImmArray we must copy it.
    */
  def slowSnoc[B >: A](el: B): ImmArray[B] = {
    val newArray: mutable.ArraySeq[B] = new mutable.ArraySeq(length + 1)
    for (i <- indices) {
      newArray(i) = uncheckedGet(i)
    }
    newArray(length) = el
    ImmArray.unsafeFromArraySeq(newArray)
  }

  /** O(min(n, m)) */
  def zip[B](that: ImmArray[B]): ImmArray[(A, B)] = {
    val newLen = Math.min(length, that.length)
    val newArray: mutable.ArraySeq[(A, B)] = new mutable.ArraySeq(newLen)
    for (i <- 0 until newLen) {
      newArray(i) = (uncheckedGet(i), that.uncheckedGet(i))
    }
    ImmArray.unsafeFromArraySeq(newArray)
  }

  /** O(1) */
  def isEmpty: Boolean = length == 0

  /** O(1) */
  def nonEmpty: Boolean = length != 0

  /** O(n) */
  def toList: List[A] = toSeq.toList

  /** O(n) */
  def toArray[B >: A: ClassTag]: Array[B] = {
    val arr: Array[B] = new Array(length)
    for (i <- indices) {
      arr(i) = uncheckedGet(i)
    }
    arr
  }

  /** O(1).
    *
    * Note: very important that this is a `scala.collection.immutable.IndexedSeq`,
    * and not a possibly mutable `scala.collection.IndexedSeq`, otherwise we could
    * return the `array` as is and people would be able to break the original
    * `ImmArray`.
    */
  def toIndexedSeq: ImmArray.ImmArraySeq[A] = new ImmArray.ImmArraySeq[A](this)

  /** O(1)
    *
    * Note: very important that this is a `scala.collection.immutable.IndexedSeq`,
    * and not a possibly mutable `scala.collection.IndexedSeq`, otherwise we could
    * return the `array` as is and people would be able to break the original
    * `ImmArray`.
    */
  def toSeq: ImmArray.ImmArraySeq[A] = new ImmArray.ImmArraySeq[A](this)

  /** O(n) */
  def collect[B](f: PartialFunction[A, B]): ImmArray[B] = {
    val builder = ImmArray.newBuilder[B]
    var i = 0
    while (i < length) {
      val a = uncheckedGet(i)
      if (f.isDefinedAt(a)) builder += f(a)
      i += 1
    }
    builder.result()
  }

  /** O(n) */
  def foreach(f: A => Unit): Unit = {
    @tailrec
    def go(cursor: Int): Unit =
      if (cursor < length) {
        f(uncheckedGet(cursor))
        go(cursor + 1)
      }
    go(0)
  }

  /** O(n) */
  def foldLeft[B](z: B)(f: (B, A) => B): B = {
    @tailrec
    def go(cursor: Int, acc: B): B = {
      if (cursor < length)
        go(cursor + 1, f(acc, uncheckedGet(cursor)))
      else
        acc
    }
    go(0, z)
  }

  /** O(n) */
  def foldRight[B](z: B)(f: (A, B) => B): B = {
    @tailrec
    def go(cursor: Int, acc: B): B =
      if (cursor >= 0)
        go(cursor - 1, f(uncheckedGet(cursor), acc))
      else
        acc
    go(length - 1, z)
  }

  /** O(n) */
  def find(f: A => Boolean): Option[A] = {
    @tailrec
    def go(i: Int): Option[A] =
      if (i < length) {
        val e = uncheckedGet(i)
        if (f(e)) Some(e)
        else go(i + 1)
      } else None
    go(0)
  }

  /** O(n) */
  def indexWhere(p: A => Boolean): Int =
    indices collectFirst {
      case i if p(uncheckedGet(i)) => i
    } getOrElse -1

  /** O(1) */
  def indices: Range = 0 until length

  /** O(1) */
  def canEqual(that: Any) = that.isInstanceOf[ImmArray[_]]

  /** O(n) */
  override def equals(that: Any): Boolean = that match {
    case thatArr: ImmArray[_] if length == thatArr.length =>
      indices forall { i =>
        uncheckedGet(i) == thatArr.uncheckedGet(i)
      }
    case _ => false
  }

  /** O(n) */
  private[data] def equalz[B >: A](thatArr: ImmArray[B])(implicit B: Equal[B]): Boolean =
    (length == thatArr.length) && {
      indices forall { i =>
        B.equal(uncheckedGet(i), thatArr.uncheckedGet(i))
      }
    }

  /** O(n) */
  override def toString: String = iterator.mkString("ImmArray(", ",", ")")

  /** O(n) */
  def filter(f: A => Boolean): ImmArray[A] =
    collect { case x if f(x) => x }

  override def hashCode(): Int = toSeq.hashCode()
}

object ImmArray extends ImmArrayInstances {
  private[this] val emptySingleton: ImmArray[Nothing] =
    ImmArray.unsafeFromArraySeq(new mutable.ArraySeq(0))
  def empty[T]: ImmArray[T] = emptySingleton

  def apply[T](element0: T, elements: T*): ImmArray[T] = {
    val builder = ImmArray.newBuilder[T]
    builder += element0
    builder ++= elements
    builder.result()
  }

  def apply[T](elements: Iterable[T]): ImmArray[T] = elements match {
    case ias: ImmArraySeq[T] => ias.toImmArray
    case _ =>
      val builder = ImmArray.newBuilder[T]
      builder ++= elements
      builder.result()
  }

  def unapplySeq[T](arr: ImmArray[T]): Option[IndexedSeq[T]] = Some(arr.toIndexedSeq)

  /** This is unsafe because if you modify the ArraySeq you are passing in after creating the ImmArray
    * you'll modify the ImmArray too, which is supposed to be immutable. If you're using it,
    * you must guarantee that the provided `Array` is not modified for the entire lifetime
    * of the resulting `ImmArray`.
    */
  def unsafeFromArraySeq[T](arr: mutable.ArraySeq[_ <: T]): ImmArray[T] =
    new ImmArray(0, arr.length, arr)

  implicit val immArrayInstance: Traverse[ImmArray] = new Traverse[ImmArray] {
    override def traverseImpl[F[_]: Applicative, A, B](immArr: ImmArray[A])(
        f: A => F[B]): F[ImmArray[B]] = {
      immArr
        .foldLeft(BackStack.empty[B].point[F]) { (ys, x) =>
          ^(ys, f(x))(_ :+ _)
        }
        .map(_.toImmArray)
    }
  }

  implicit def immArrayOrderInstance[A: Order]: Order[ImmArray[A]] = {
    import scalaz.std.iterable._
    orderBy(ia => toIterableForScalazInstances(ia.iterator), true)
  }

  private[ImmArray] final class IACanBuildFrom[A]
      extends CanBuildFrom[ImmArray[_], A, ImmArray[A]] {
    override def apply(from: ImmArray[_]) = newBuilder[A]
    override def apply() = newBuilder[A]
  }

  implicit def `ImmArray canBuildFrom`[A]: CanBuildFrom[ImmArray[_], A, ImmArray[A]] =
    new IACanBuildFrom

  def newBuilder[A]: mutable.Builder[A, ImmArray[A]] =
    mutable.ArraySeq.newBuilder[A].mapResult(ImmArray.unsafeFromArraySeq)

  /** Note: we define this purely to be able to write `toSeq`. We cannot return
    * `ArraySeq` directly because that's a mutable `Seq`, which would allow people
    * to break the invariants.
    *
    * However, _do not_ use it for anything but defining interface where you need
    * to expose a `Seq`, and you also need to use implicits that refer to the
    * specific types, such as the traverse instance.
    */
  final class ImmArraySeq[+A] private[ImmArray] (array: ImmArray[A])
      extends IndexedSeq[A]
      with GenericTraversableTemplate[A, ImmArraySeq]
      with IndexedSeqLike[A, ImmArraySeq[A]]
      with IndexedSeqOptimized[A, ImmArraySeq[A]] {
    import ImmArraySeq.IASCanBuildFrom

    // TODO make this faster by implementing as many methods as possible.
    override def iterator: Iterator[A] = array.iterator
    override def reverseIterator: Iterator[A] = array.reverseIterator

    override def apply(idx: Int): A = array(idx)

    override def length: Int = array.length

    override def head: A = array.head
    override def tail: ImmArraySeq[A] = new ImmArraySeq(array.tail)
    override def last: A = array.last
    override def init: ImmArraySeq[A] = new ImmArraySeq(array.init)
    override def slice(from: Int, to: Int): ImmArraySeq[A] =
      new ImmArraySeq(array.relaxedSlice(from, to))
    override def copyToArray[B >: A](xs: Array[B], dstStart: Int, dstLen: Int): Unit =
      array.copyToArray(xs, dstStart, dstLen)

    override def map[B, That](f: A => B)(implicit bf: CanBuildFrom[ImmArraySeq[A], B, That]): That =
      bf match {
        case _: IASCanBuildFrom[B] => array.map(f).toSeq
        case _ => super.map(f)(bf)
      }

    override def to[Col[_]](implicit bf: CanBuildFrom[Nothing, A, Col[A @uncheckedVariance]])
      : Col[A @uncheckedVariance] =
      bf match {
        case _: IASCanBuildFrom[A] => this
        case _: IACanBuildFrom[A] => toImmArray
        case _: FrontStack.FSCanBuildFrom[A] => FrontStack(toImmArray)
        case _ => super.to(bf)
      }

    override def companion: GenericCompanion[ImmArraySeq] = ImmArraySeq

    def toImmArray: ImmArray[A] = array
  }

  object ImmArraySeq extends IndexedSeqFactory[ImmArraySeq] {
    implicit val immArraySeqInstance: Traverse[ImmArraySeq] = new Traverse[ImmArraySeq]
    with Foldable.FromFoldr[ImmArraySeq] {
      override def map[A, B](fa: ImmArraySeq[A])(f: A => B) = fa.toImmArray.map(f).toSeq
      override def foldLeft[A, B](fa: ImmArraySeq[A], z: B)(f: (B, A) => B) =
        fa.foldLeft(z)(f)
      override def foldRight[A, B](fa: ImmArraySeq[A], z: => B)(f: (A, => B) => B) =
        fa.foldRight(z)(f(_, _))
      override def traverseImpl[F[_], A, B](immArr: ImmArraySeq[A])(f: A => F[B])(
          implicit F: Applicative[F]): F[ImmArraySeq[B]] = {
        F.map(immArr.foldLeft[F[BackStack[B]]](F.point(BackStack.empty)) {
          case (ys, x) => F.apply2(ys, f(x))(_ :+ _)
        })(_.toImmArray.toSeq)
      }
    }

    implicit def `immArraySeq Equal instance`[A: Equal]: Equal[ImmArraySeq[A]] =
      equalBy(_.toImmArray, true)

    private final class IASCanBuildFrom[A] extends GenericCanBuildFrom[A]

    implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, ImmArraySeq[A]] =
      new IASCanBuildFrom

    override def newBuilder[A]: mutable.Builder[A, ImmArraySeq[A]] =
      ImmArray.newBuilder.mapResult(_.toSeq)
  }
}

sealed abstract class ImmArrayInstances {
  implicit def immArrayEqualInstance[A: Equal]: Equal[ImmArray[A]] =
    ScalazEqual.withNatural(Equal[A].equalIsNatural)(_ equalz _)
}

/** We do not provide apply on purpose -- see slowCons on why we want to discourage consing */
object ImmArrayCons {

  /** O(1) */
  def unapply[T](xs: ImmArray[T]): Option[(T, ImmArray[T])] =
    if (xs.isEmpty) {
      None
    } else {
      Some((xs.head, xs.tail))
    }
}

/** We do not provide apply on purpose -- see slowCons on why we want to discourage snocing */
object ImmArraySnoc {

  /** O(1) */
  def unapply[T](xs: ImmArray[T]): Option[(ImmArray[T], T)] =
    if (xs.isEmpty) {
      None
    } else {
      Some((xs.init, xs.last))
    }
}
