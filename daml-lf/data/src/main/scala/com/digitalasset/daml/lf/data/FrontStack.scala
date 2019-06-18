// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.annotation.tailrec
import FrontStack.{FQ, FQCons, FQEmpty, FQPrepend}

import scalaz.Equal

import scala.collection.mutable

/** A stackk which allows to cons, prepend, and pop in constant time, and generate an ImmArray in linear time.
  * Very useful when needing to traverse stuff in topological order or similar situations.
  */
final class FrontStack[+A] private (fq: FQ[A], len: Int) {

  /** O(1) */
  def length: Int = len

  /** O(1) */
  def +:[B >: A](x: B): FrontStack[B] = new FrontStack(FQCons(x, fq), len + 1)

  /** O(1) */
  def ++:[B >: A](xs: ImmArray[B]): FrontStack[B] =
    if (xs.length > 0) {
      new FrontStack(FQPrepend(xs, fq), len + xs.length)
    } else {
      this
    }

  /** O(n) */
  def toImmArray: ImmArray[A] = {
    val array = new mutable.ArraySeq[A](len)

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

    ImmArray.unsafeFromArraySeq(array)
  }

  /** O(1) */
  def pop: Option[(A, FrontStack[A])] = {
    if (len > 0) {
      fq match {
        case FQEmpty => throw new RuntimeException(s"FrontStack has length $len but FQEmpty.")
        case FQCons(head, tail) => Some((head, new FrontStack(tail, len - 1)))
        case FQPrepend(head, tail) =>
          if (head.length > 1) {
            Some((head.head, new FrontStack(FQPrepend(head.tail, tail), len - 1)))
          } else if (head.length > 0) {
            Some((head.head, new FrontStack(tail, len - 1)))
          } else {
            throw new RuntimeException(s"FrontStack had FQPrepend with non-empty head: $head")
          }
      }
    } else {
      None
    }
  }

  /** O(n) */
  def map[B](f: A => B): FrontStack[B] = {
    this.toImmArray.map(f) ++: FrontStack.empty
  }

  /** O(1) */
  def isEmpty: Boolean = len == 0

  /** O(1) */
  def nonEmpty: Boolean = len > 0

  /** O(1) */
  def iterator: Iterator[A] = {
    val that = this
    new Iterator[A] {
      var queue: FrontStack[A] = that

      override def next(): A = {
        val Some((head, tail)) = queue.pop
        queue = tail
        head
      }

      override def hasNext: Boolean = queue.nonEmpty
    }
  }

  /** O(n) */
  def foreach(f: A => Unit): Unit = this.iterator.foreach(f)

  /** O(1) */
  def canEqual(that: Any) = that.isInstanceOf[FrontStack[A]]

  /** O(n) */
  override def equals(that: Any) = that match {
    case thatQueue: FrontStack[A] => this.iterator sameElements thatQueue.iterator
    case _ => false
  }

  override def hashCode(): Int = toImmArray.hashCode()

  /** O(n) */
  override def toString: String = "FrontStack(" + iterator.map(_.toString).mkString(",") + ")"
}

object FrontStack {
  private[this] val emptySingleton: FrontStack[Nothing] = new FrontStack(FQEmpty, 0)
  def empty[A]: FrontStack[A] = emptySingleton

  def apply[A](xs: ImmArray[A]): FrontStack[A] = xs ++: empty

  def apply[T](element: T, elements: T*): FrontStack[T] =
    element +: apply(ImmArray(elements))

  def apply[T](elements: Seq[T]): FrontStack[T] =
    apply(ImmArray(elements))

  def unapply[T](xs: FrontStack[T]): Boolean = xs.isEmpty

  implicit def equalInstance[A](implicit A: Equal[A]): Equal[FrontStack[A]] =
    ScalazEqual.withNatural(Equal[A].equalIsNatural) { (as, bs) =>
      val ai = as.iterator
      val bi = bs.iterator
      @tailrec def go(): Boolean =
        if (ai.hasNext) bi.hasNext && A.equal(ai.next, bi.next) && go()
        else !bi.hasNext
      go()
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
