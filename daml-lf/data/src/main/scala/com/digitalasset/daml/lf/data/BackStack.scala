// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.annotation.tailrec
import BackStack.{BQ, BQAppend, BQEmpty, BQSnoc}

import scala.collection.mutable

/** A stack which allows to snoc, append, and pop in constant time, and generate an ImmArray in linear time.
  */
final class BackStack[+A] private (fq: BQ[A], len: Int) {

  /** O(1) */
  def :+[B >: A](x: B): BackStack[B] = new BackStack(BQSnoc(fq, x), len + 1)

  /** O(1) */
  def :++[B >: A](xs: ImmArray[B]): BackStack[B] =
    if (xs.length > 0) {
      new BackStack(BQAppend(fq, xs), len + xs.length)
    } else {
      this
    }

  /** O(n) */
  def toImmArray: ImmArray[A] = {
    val array = new mutable.ArraySeq[A](len)

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
    go(len - 1, fq)

    ImmArray.unsafeFromArraySeq(array)
  }

  /** O(1) */
  def pop: Option[(BackStack[A], A)] = {
    if (len > 0) {
      fq match {
        case BQEmpty => throw new RuntimeException(s"BackQueue has length $len but BQEmpty.")
        case BQSnoc(init, last) => Some((new BackStack(init, len - 1), last))
        case BQAppend(init, last) =>
          if (last.length > 1) {
            Some((new BackStack(BQAppend(init, last.init), len - 1), last.last))
          } else if (last.length > 0) {
            Some((new BackStack(init, len - 1), last.last))
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
  def isEmpty: Boolean = len == 0

  /** O(1) */
  def nonEmpty: Boolean = len > 0

  /** O(1)
    *
    * Note that this iterator produces the element in reverse order.
    */
  def reverseIterator: Iterator[A] = {
    val that = this
    new Iterator[A] {
      var queue: BackStack[A] = that

      override def next(): A = {
        val Some((init, last)) = queue.pop
        queue = init
        last
      }

      override def hasNext: Boolean = queue.nonEmpty
    }
  }

  /** O(n) */
  def reverseForeach(f: A => Unit): Unit = this.reverseIterator.foreach(f)

  /** O(1) */
  def canEqual(that: Any) = that.isInstanceOf[BackStack[A]]

  /** O(n) */
  override def equals(that: Any) = that match {
    case thatQueue: BackStack[A] => this.reverseIterator sameElements thatQueue.reverseIterator
    case _ => false
  }

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
    apply(ImmArray(element +: elements))

  def apply[T](elements: Seq[T]): BackStack[T] =
    apply(ImmArray(elements))

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
