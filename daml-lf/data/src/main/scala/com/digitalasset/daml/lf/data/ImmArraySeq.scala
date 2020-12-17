// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.equalBy

import scalaz.{Applicative, Equal, Foldable, Traverse}

import scala.collection.compat.immutable.ArraySeq

/** Note: we define this purely to be able to write `toSeq`.
  *
  * However, _do not_ use it for anything but defining interface where you need
  * to expose a `Seq`, and you also need to use implicits that refer to the
  * specific types, such as the traverse instance.
  */
final class ImmArraySeq[+A](array: ImmArray[A]) extends AbstractImmArraySeq[A](array) {

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

  def toImmArray: ImmArray[A] = array
}

object ImmArraySeq extends ImmArraySeqCompanion {
  implicit val `immArraySeq Traverse instance`: Traverse[ImmArraySeq] = new Traverse[ImmArraySeq]
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

  // Here only for 2.12 (harmless in 2.13); placed in ImmArraySeqCompanion the
  // implicit gets in an unwinnable fight with IndexedSeq's version
  override implicit def canBuildFrom[A]: Factory[A] = super.canBuildFrom
}
