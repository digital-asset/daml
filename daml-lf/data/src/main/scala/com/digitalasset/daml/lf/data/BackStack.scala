// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import BackStack.{BQ, BQAppend, BQEmpty, BQSnoc}

import scala.annotation.tailrec

/** A stack which allows to snoc, append, and pop in constant time, and generate an ImmArray in linear time.
  */
final class BackStack[+A] private (fq: BQ[A], val length: Int) {

  /** O(1) */
  def :+[B >: A](x: B): BackStack[B] = new BackStack(BQSnoc(fq, x), length + 1)

  /** O(1) */
  def :++[B >: A](xs: ImmArray[B]): BackStack[B] =
    if (xs.length > 0) {
      new BackStack(BQAppend(fq, xs), length + xs.length)
    } else {
      this
    }

  /** O(n) */
  def toImmArray: ImmArray[A] = {
    val array = new Array[Any](length)

    @tailrec
    def go(cursor: Int, fq: BQ[A]): Unit = fq match {
      case BQEmpty => ()
      case BQSnoc(init, last) =>
        array(cursor) = last
        go(cursor - 1, init)
      case BQAppend(init, last) =>
        for (i <- last.indices) {
          array(cursor - i) = last(last.length - 1 - i)
        }
        go(cursor - last.length, init)
    }

    go(length - 1, fq)

    ImmArray.unsafeFromArray(array)
  }

  /** O(n) */
  def toFrontStack: FrontStack[A] = {
    @tailrec
    def go(self: BQ[A], acc: FrontStack[A]): FrontStack[A] =
      self match {
        case BQSnoc(init, last) => go(init, last +: acc)
        case BQAppend(init, last) => go(init, last ++: acc)
        case BQEmpty => acc
      }
    go(fq, FrontStack.Empty)
  }

  /** O(1) */
  def pop: Option[(BackStack[A], A)] = {
    if (length > 0) {
      fq match {
        case BQEmpty => throw new RuntimeException(s"BackQueue has length $length but BQEmpty.")
        case BQSnoc(init, last) => Some((new BackStack(init, length - 1), last))
        case BQAppend(init, last) =>
          if (last.length > 1) {
            Some((new BackStack(BQAppend(init, last.init), length - 1), last.last))
          } else if (last.length > 0) {
            Some((new BackStack(init, length - 1), last.last))
          } else {
            throw new RuntimeException(s"BackQueue had BQPrepend with non-empty last: $last")
          }
      }
    } else {
      None
    }
  }

  /** O(n) */
  def map[B](f: A => B): BackStack[B] = {
    BackStack(this.toImmArray.map(f))
  }

  /** O(1) */
  def isEmpty: Boolean = length == 0

  /** O(1) */
  def nonEmpty: Boolean = length > 0

  /** O(1)
    *
    * Note that this iterator produces the element in reverse order.
    */
  def reverseIterator: Iterator[A] = {
    val that = this
    new Iterator[A] {
      var queue: BackStack[A] = that

      override def next(): A = queue.pop match {
        case Some((init, last)) =>
          queue = init
          last
        case None =>
          throw new NoSuchElementException("head of empty list")
      }

      override def hasNext: Boolean = queue.nonEmpty
    }
  }

  /** Fold over the steps in the BQ structure. Subject to change on a whim. */
  private[data] def bqFoldRight[Z](z: Z)(snoc: (A, Z) => Z, append: (ImmArray[A], Z) => Z): Z = {
    @tailrec def go(self: BQ[A], z: Z): Z = self match {
      case BQSnoc(init, last) => go(init, snoc(last, z))
      case BQAppend(init, last) => go(init, append(last, z))
      case BQEmpty => z
    }
    go(fq, z)
  }

  /** O(n) */
  def reverseForeach(f: A => Unit): Unit = this.reverseIterator.foreach(f)

  /** O(1) */
  def canEqual(that: Any) = that.isInstanceOf[BackStack[_]]

  /** O(n) */
  override def equals(that: Any) = that match {
    case thatQueue: BackStack[_] =>
      this.reverseIterator.sameElements[Any](thatQueue.reverseIterator)
    case _ => false
  }

  override def hashCode(): Int = toImmArray.hashCode()

  /** O(n) */
  override def toString: String =
    "BackQueue(" + toImmArray.iterator.map(_.toString).mkString(",") + ")"
}

object BackStack {
  private[this] val emptySingleton: BackStack[Nothing] = new BackStack(BQEmpty, 0)
  def empty[A]: BackStack[A] = emptySingleton
  def singleton[A](x: A): BackStack[A] = empty :+ x

  def apply[A](xs: ImmArray[A]): BackStack[A] = empty :++ xs

  def apply[T](element: T, elements: T*): BackStack[T] =
    empty :+ element :++ elements.to(ImmArray)

  def apply[T](elements: Seq[T]): BackStack[T] =
    apply(elements.to(ImmArray))

  def unapply[T](xs: BackStack[T]): Boolean = xs.isEmpty

  private sealed trait BQ[+A]
  private case object BQEmpty extends BQ[Nothing]
  private final case class BQSnoc[A](init: BQ[A], last: A) extends BQ[A]
  // INVARIANT: last is non-empty
  private final case class BQAppend[A](init: BQ[A], last: ImmArray[A]) extends BQ[A]
}

object BackStackSnoc {
  def unapply[A](xs: BackStack[A]): Option[(BackStack[A], A)] = xs.pop
}
