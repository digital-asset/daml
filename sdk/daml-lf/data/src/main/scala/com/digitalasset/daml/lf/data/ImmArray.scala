// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import com.daml.scalautil.FoldableContravariant
import com.daml.scalautil.Statement.discard

import ScalazEqual.{equalBy, orderBy, toIterableForScalazInstances}
import scalaz.syntax.applicative._
import scalaz.{Applicative, Equal, Foldable, Order, Traverse}

import scala.annotation.tailrec
import scala.collection.StrictOptimizedSeqFactory
import scala.collection.immutable.{AbstractSeq, ArraySeq, IndexedSeqOps, StrictOptimizedSeqOps}
import scala.collection.mutable.Builder
import scala.reflect.ClassTag

/** Simple immutable array. The intention is that all the operations have the "obvious"
  * operational behavior (like Vector in haskell).
  *
  * Functions that slice the `ImmArray` (including `head`, `tail`, etc) are all constant time --
  * they keep referring to the same underlying array.
  *
  * Note that we _very intentionally_ do _not_ make this an instance of any sorts of `Seq`, since
  * using `Seq` encourages patterns where the performance of what you're doing is totally
  * unclear. Use `toSeq` if you want a `Seq`, and think about what that means.
  */
final class ImmArray[+A] private (
    private val start: Int,
    val length: Int,
    array: ArraySeq[A],
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

  /** O(1) */
  def get(idx: Int): Option[A] =
    if (idx >= length) {
      None
    } else {
      Some(uncheckedGet(idx))
    }

  private def uncheckedGet(idx: Int): A = array(start + idx)

  /** O(n) */
  def copyToArray[B >: A](dst: Array[B], dstStart: Int, dstLen: Int): Int = {
    val numElems = Math.min(length, dstLen)
    for (i <- 0 until numElems) {
      dst(dstStart + i) = uncheckedGet(i)
    }
    numElems
  }

  /** O(n) */
  def copyToArray[B >: A](xs: Array[B]): Int = {
    copyToArray(xs, 0, xs.length)
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
        s"strictSlice arguments out of bounds for ImmArray. length: $length, from: $from, until: $until"
      )
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
      ImmArray.Empty
    } else {
      new ImmArray(start + from, newLen, array)
    }
  }

  /** O(n) */
  def map[B](f: A => B): ImmArray[B] = {
    val newArray: Array[Any] = new Array(length)
    for (i <- indices) {
      newArray(i) = f(uncheckedGet(i))
    }
    ImmArray.unsafeFromArray[B](newArray)
  }

  /** O(n) */
  def reverse: ImmArray[A] = {
    val newArray: Array[Any] = new Array(length)
    for (i <- indices) {
      newArray(i) = array(start + length - (i + 1))
    }
    ImmArray.unsafeFromArray[A](newArray)
  }

  /** O(n+m)
    *
    * If you find using this a lot, consider using another data structure (maybe FrontQueue or BackQueue),
    * since to append ImmArray we must copy both of them.
    */
  def slowAppend[B >: A](other: ImmArray[B]): ImmArray[B] = {
    val newArray: Array[Any] = new Array(length + other.length)
    for (i <- indices) {
      newArray(i) = uncheckedGet(i)
    }
    for (i <- other.indices) {
      newArray(length + i) = other.uncheckedGet(i)
    }
    ImmArray.unsafeFromArray[B](newArray)
  }

  /** O(n)
    *
    * If you find yourself using this a lot, consider using another data structure,
    * since to cons an ImmArray we must copy it.
    */
  def slowCons[B >: A](el: B): ImmArray[B] = {
    val newArray: Array[Any] = new Array(length + 1)
    newArray(0) = el
    for (i <- indices) {
      newArray(i + 1) = uncheckedGet(i)
    }
    ImmArray.unsafeFromArray(newArray)
  }

  /** O(n)
    *
    * If you find yourself using this a lot, consider using another data structure,
    * since to snoc an ImmArray we must copy it.
    */
  def slowSnoc[B >: A](el: B): ImmArray[B] = {
    val newArray: Array[Any] = new Array(length + 1)
    for (i <- indices) {
      newArray(i) = uncheckedGet(i)
    }
    newArray(length) = el
    ImmArray.unsafeFromArray(newArray)
  }

  /** O(min(n, m)) */
  def zip[B](that: ImmArray[B]): ImmArray[(A, B)] = {
    val newLen = Math.min(length, that.length)
    val newArray: Array[Any] = new Array(newLen)
    for (i <- 0 until newLen) {
      newArray(i) = (uncheckedGet(i), that.uncheckedGet(i))
    }
    ImmArray.unsafeFromArray(newArray)
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
  def toIndexedSeq: ImmArray.ImmArraySeq[A] = new ImmArray.ImmArraySeq(this)

  /** O(1)
    *
    * Note: very important that this is a `scala.collection.immutable.IndexedSeq`,
    * and not a possibly mutable `scala.collection.IndexedSeq`, otherwise we could
    * return the `array` as is and people would be able to break the original
    * `ImmArray`.
    */
  def toSeq: ImmArray.ImmArraySeq[A] = toIndexedSeq

  /** O(n) */
  def collect[B](f: PartialFunction[A, B]): ImmArray[B] = {
    val builder = ImmArray.newBuilder[B]
    var i = 0
    while (i < length) {
      val a = uncheckedGet(i)
      if (f.isDefinedAt(a)) discard(builder += f(a))
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

  def toFrontStack: FrontStack[A] = FrontStack.from(this)

}

object ImmArray extends scala.collection.IterableFactory[ImmArray] {

  val Empty: ImmArray[Nothing] = ImmArray.fromArraySeq(ArraySeq.empty)

  def empty[T]: ImmArray[Nothing] = Empty

  def unapplySeq[T](arr: ImmArray[T]): Option[IndexedSeq[T]] = Some(arr.toIndexedSeq)

  /** This is unsafe because if you modify the Array you are passing in after creating the ImmArray
    * you'll modify the ImmArray too, which is supposed to be immutable. If you're using it,
    * you must guarantee that the provided `Array` is not modified for the entire lifetime
    * of the resulting `ImmArray`.
    */
  def unsafeFromArray[T](arr: Array[Any]): ImmArray[T] = {
    // ArraySeq can be boxed or unboxed. By requiring an Array[Any] we enforce
    // that values are always boxed.
    new ImmArray(0, arr.length, ArraySeq.unsafeWrapArray(arr).asInstanceOf[ArraySeq[T]])
  }

  def fromArraySeq[T](arr: ArraySeq[T]): ImmArray[T] =
    new ImmArray(0, arr.length, arr)

  implicit val immArrayInstance: Traverse[ImmArray] = new Traverse[ImmArray] {
    override def traverseImpl[F[_]: Applicative, A, B](
        immArr: ImmArray[A]
    )(f: A => F[B]): F[ImmArray[B]] = {
      immArr
        .foldLeft(BackStack.empty[B].point[F]) { (ys, x) =>
          ^(ys, f(x))(_ :+ _)
        }
        .map(_.toImmArray)
    }

    override def foldLeft[A, B](fa: ImmArray[A], z: B)(f: (B, A) => B) =
      fa.foldLeft(z)(f)

    override def foldRight[A, B](fa: ImmArray[A], z: => B)(f: (A, => B) => B) =
      fa.foldRight(z)(f(_, _))
  }

  implicit def immArrayOrderInstance[A: Order]: Order[ImmArray[A]] = {
    import scalaz.std.iterable._
    orderBy(ia => toIterableForScalazInstances(ia.iterator), true)
  }

  implicit def immArrayEqualInstance[A: Equal]: Equal[ImmArray[A]] =
    ScalazEqual.withNatural(Equal[A].equalIsNatural)(_ equalz _)

  override def from[A](it: IterableOnce[A]): ImmArray[A] = {
    it match {
      case arraySeq: ImmArray.ImmArraySeq[A] =>
        arraySeq.toImmArray
      case otherwise =>
        val builder = newBuilder[A]
        builder.sizeHint(otherwise)
        builder
          .addAll(otherwise)
          .result()
    }
  }

  override def newBuilder[A]: Builder[A, ImmArray[A]] =
    ArraySeq
      .newBuilder[Any]
      .asInstanceOf[Builder[A, ArraySeq[A]]]
      .mapResult(ImmArray.fromArraySeq(_))

  /** Note: we define this purely to be able to write `toSeq`.
    *
    * However, _do not_ use it for anything but defining interface where you need
    * to expose a `Seq`, and you also need to use implicits that refer to the
    * specific types, such as the traverse instance.
    */
  final class ImmArraySeq[+A](array: ImmArray[A])
      extends AbstractSeq[A]
      with IndexedSeq[A]
      with IndexedSeqOps[A, ImmArraySeq, ImmArraySeq[A]]
      with StrictOptimizedSeqOps[A, ImmArraySeq, ImmArraySeq[A]]
      with scala.collection.IterableFactoryDefaults[A, ImmArraySeq] {

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

    override final def iterableFactory: scala.collection.SeqFactory[ImmArraySeq] = ImmArraySeq

    override final def copyToArray[B >: A](xs: Array[B], dstStart: Int, dstLen: Int): Int =
      array.copyToArray(xs, dstStart, dstLen)

    def toImmArray: ImmArray[A] = array
  }

  object ImmArraySeq extends StrictOptimizedSeqFactory[ImmArraySeq] {
    type Factory[A] = Unit
    final def canBuildFrom[A]: Factory[A] = ()
    final def empty[A]: ImmArraySeq[Nothing] = Empty
    final def from[E](it: IterableOnce[E]): ImmArraySeq[E] =
      ImmArray.newBuilder.addAll(it).result().toSeq
    final def newBuilder[A] = ImmArray.newBuilder.mapResult(_.toSeq)

    val Empty: ImmArraySeq[Nothing] = ImmArray.Empty.toSeq
    implicit val `immArraySeq Traverse instance`: Traverse[ImmArraySeq] = new Traverse[ImmArraySeq]
      with Foldable.FromFoldr[ImmArraySeq]
      with FoldableContravariant[ImmArraySeq, ImmArray] {
      override def map[A, B](fa: ImmArraySeq[A])(f: A => B) = fa.toImmArray.map(f).toSeq

      protected[this] override def Y = Foldable[ImmArray]
      protected[this] override def ctmap[A](xa: ImmArraySeq[A]) = xa.toImmArray

      override def traverseImpl[F[_], A, B](
          immArr: ImmArraySeq[A]
      )(f: A => F[B])(implicit F: Applicative[F]): F[ImmArraySeq[B]] = {
        F.map(immArr.foldLeft[F[BackStack[B]]](F.point(BackStack.empty)) { case (ys, x) =>
          F.apply2(ys, f(x))(_ :+ _)
        })(_.toImmArray.toSeq)
      }
    }

    implicit def `immArraySeq Equal instance`[A: Equal]: Equal[ImmArraySeq[A]] =
      equalBy(_.toImmArray, true)
  }
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
