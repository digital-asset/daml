// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.{equalBy}

import scalaz.{Applicative, Equal, Foldable, Traverse}

import scala.collection.StrictOptimizedSeqFactory
import scala.collection.immutable.{AbstractSeq, IndexedSeqOps, StrictOptimizedSeqOps}

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

  override def iterableFactory: scala.collection.SeqFactory[ImmArraySeq] = ImmArraySeq

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
  override def copyToArray[B >: A](xs: Array[B], dstStart: Int, dstLen: Int): Int =
    array.copyToArray(xs, dstStart, dstLen)
  def toImmArray: ImmArray[A] = array
}

object ImmArraySeq extends StrictOptimizedSeqFactory[ImmArraySeq] {
  def empty[A] = ImmArray.empty.toSeq
  def from[E](it: IterableOnce[E]) = (ImmArray.newBuilder ++= it).result().toSeq
  def newBuilder[A] = ImmArray.newBuilder.mapResult(_.toSeq)

  implicit val `immArraySeq Traverse instance`: Traverse[ImmArraySeq] = new Traverse[ImmArraySeq]
  with Foldable.FromFoldr[ImmArraySeq] {
    // import Implicits._
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

  implicit def `immArraySeq Equal instance`[A: Equal]: Equal[ImmArraySeq[A]] = {
    equalBy(_.toImmArray, true)
  }

}
