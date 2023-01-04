// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.{equalBy, orderBy, toIterableForScalazInstances}
import scalaz.{Applicative, Equal, Order, Traverse}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/** A stack which allows to cons, prepend, and pop in constant time, and generate an ImmArray in linear time.
  * Very useful when needing to traverse stuff in topological order or similar situations.
  */
final class FrontStack[+A] private (fq: FrontStack.FQ[A], val length: Int) {

  import FrontStack._

  /** O(n) */
  @throws[IndexOutOfBoundsException]
  def slowApply(ix: Int): A = {
    if (ix < 0) throw new IndexOutOfBoundsException(ix.toString)
    val i = iterator

    @tailrec def lp(ix: Int): A =
      if (!i.hasNext) throw new IndexOutOfBoundsException(ix.toString)
      else {
        val v = i.next()
        if (ix <= 0) v else lp(ix - 1)
      }
    lp(ix)
  }

  /** O(1) */
  def +:[B >: A](x: B): FrontStack[B] = new FrontStack(FQCons(x, fq), length + 1)

  /** O(1) */
  def ++:[B >: A](xs: ImmArray[B]): FrontStack[B] =
    if (xs.length > 0) {
      new FrontStack(FQPrepend(xs, fq), length + xs.length)
    } else {
      this
    }

  /** O(n) */
  def toImmArray: ImmArray[A] = {
    val array = new Array[Any](length)

    @tailrec
    def go(cursor: Int, fq: FQ[A]): Unit = fq match {
      case FQEmpty => ()
      case FQCons(head, tail) =>
        array(cursor) = head
        go(cursor + 1, tail)
      case FQPrepend(head, tail) =>
        for (i <- head.indices) {
          array(cursor + i) = head(i)
        }
        go(cursor + head.length, tail)
    }
    go(0, fq)

    ImmArray.unsafeFromArray[A](array)
  }

  /** O(1) */
  def pop: Option[(A, FrontStack[A])] = {
    if (length > 0) {
      fq match {
        case FQEmpty => throw new RuntimeException(s"FrontStack has length $length but FQEmpty.")
        case FQCons(head, tail) => Some((head, new FrontStack(tail, length - 1)))
        case FQPrepend(head, tail) =>
          if (head.length > 1) {
            Some((head.head, new FrontStack(FQPrepend(head.tail, tail), length - 1)))
          } else {
            // NOTE: We maintain the invariant that `head` is never empty.
            Some((head.head, new FrontStack(tail, length - 1)))
          }
      }
    } else {
      None
    }
  }

  /** O(n) */
  def map[B](f: A => B): FrontStack[B] = from(toImmArray.map(f))

  /** O(1) */
  def isEmpty: Boolean = length == 0

  /** O(1) */
  def nonEmpty: Boolean = length > 0

  /** O(1) */
  def iterator: Iterator[A] = {
    val that = this
    new Iterator[A] {
      var queue: FrontStack[A] = that

      override def next(): A = queue.pop match {
        case Some((head, tail)) =>
          queue = tail
          head
        case None =>
          throw new NoSuchElementException("head of empty list")
      }

      override def hasNext: Boolean = queue.nonEmpty
    }
  }

  /** O(n) */
  def toBackStack: BackStack[A] = {
    @tailrec
    def go(acc: BackStack[A], self: FQ[A]): BackStack[A] =
      self match {
        case FQCons(head, tail) => go(acc :+ head, tail)
        case FQPrepend(head, tail) => go(acc :++ head, tail)
        case FQEmpty => acc
      }
    go(BackStack.empty, fq)
  }

  /** O(n) */
  def foreach(f: A => Unit): Unit = this.iterator.foreach(f)

  /** O(1) */
  def canEqual(that: Any) = that.isInstanceOf[FrontStack[_]]

  /** O(n) */
  override def equals(that: Any) = that match {
    case thatQueue: FrontStack[_] =>
      this.length == thatQueue.length && this.iterator.sameElements[Any](thatQueue.iterator)
    case _ => false
  }

  override def hashCode(): Int =
    MurmurHash3.orderedHash(toIterableForScalazInstances(iterator))

  /** O(n) */
  override def toString: String = "FrontStack(" + iterator.map(_.toString).mkString(",") + ")"
}

object FrontStack extends scala.collection.IterableFactory[FrontStack] {
  val Empty: FrontStack[Nothing] = new FrontStack(FQEmpty, 0)

  def empty[A]: FrontStack[A] = FrontStack.Empty

  def from[A](xs: ImmArray[A]): FrontStack[A] =
    if (xs.isEmpty) Empty else new FrontStack(FQPrepend(xs, FQEmpty), length = xs.length)

  override def from[A](it: IterableOnce[A]): FrontStack[A] =
    FrontStack.from(ImmArray.from(it))

  def unapply[T](xs: FrontStack[T]): Boolean = xs.isEmpty

  override def newBuilder[A]: mutable.Builder[A, FrontStack[A]] =
    ImmArray.newBuilder.mapResult(arr => FrontStack.from(arr))
  implicit def equalInstance[A: Equal]: Equal[FrontStack[A]] = {
    import scalaz.std.iterable._
    equalBy(fs => toIterableForScalazInstances(fs.iterator), true)
  }

  implicit val `FrontStack covariant`: Traverse[FrontStack] = new Traverse[FrontStack] {
    override def traverseImpl[G[_]: Applicative, A, B](
        fa: FrontStack[A]
    )(f: A => G[B]): G[FrontStack[B]] = {
      import scalaz.syntax.applicative._, scalaz.syntax.traverse._
      fa.toBackStack.bqFoldRight(FrontStack.empty[B].pure[G])(
        (a, z) => ^(f(a), z)(_ +: _),
        (iaa, z) => ^(iaa traverse f, z)(_ ++: _),
      )
    }

    override def map[A, B](fa: FrontStack[A])(f: A => B) = fa map f

    override def foldLeft[A, B](fa: FrontStack[A], z: B)(f: (B, A) => B) =
      fa.iterator.foldLeft(z)(f)

    override def foldRight[A, B](fa: FrontStack[A], z: => B)(f: (A, => B) => B) =
      fa.toBackStack.bqFoldRight(z)(
        (a, z) => f(a, z),
        (iaa, z) => iaa.foldRight(z)(f(_, _)),
      )

    override def length[A](fa: FrontStack[A]) = fa.length
  }

  implicit def `FrontStack Order`[A: Order]: Order[FrontStack[A]] = {
    import scalaz.std.iterable._
    orderBy(fs => toIterableForScalazInstances(fs.iterator), true)
  }

  private sealed trait FQ[+A]
  private case object FQEmpty extends FQ[Nothing]
  private final case class FQCons[A](head: A, tail: FQ[A]) extends FQ[A]
  // INVARIANT: head is non-empty
  private final case class FQPrepend[A](head: ImmArray[A], tail: FQ[A]) extends FQ[A]
}

object FrontStackCons {
  def unapply[A](xs: FrontStack[A]): Option[(A, FrontStack[A])] = xs.pop
}
