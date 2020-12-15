// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.{equalBy}

import scalaz.{Applicative, Equal, Foldable, Traverse}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.compat.immutable.ArraySeq
import scala.collection.generic.{
  CanBuildFrom,
  GenericCompanion,
  GenericTraversableTemplate,
  IndexedSeqFactory
}
import scala.collection.{IndexedSeqLike, IndexedSeqOptimized, mutable}

/** Note: we define this purely to be able to write `toSeq`.
  *
  * However, _do not_ use it for anything but defining interface where you need
  * to expose a `Seq`, and you also need to use implicits that refer to the
  * specific types, such as the traverse instance.
  */
final class ImmArraySeq[+A](array: ImmArray[A])
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
  override def copyToArray[B >: A](xs: Array[B], dstStart: Int, dstLen: Int): Unit = {
    array.copyToArray(xs, dstStart, dstLen)
    ()
  }

  override def map[B, That](f: A => B)(implicit bf: CanBuildFrom[ImmArraySeq[A], B, That]): That =
    bf match {
      case _: IASCanBuildFrom[B] => array.map(f).toSeq
      case _ => super.map(f)(bf)
    }

  override def to[Col[_]](
      implicit bf: CanBuildFrom[Nothing, A, Col[A @uncheckedVariance]]): Col[A @uncheckedVariance] =
    bf match {
      case _: IASCanBuildFrom[A] => this
      case _: ImmArrayInstances.IACanBuildFrom[A] => toImmArray
      case _: FrontStackInstances.FSCanBuildFrom[A] => FrontStack(toImmArray)
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

  private[data] final class IASCanBuildFrom[A] extends GenericCanBuildFrom[A]

  implicit def canBuildFrom[A]: CanBuildFrom[Coll, A, ImmArraySeq[A]] =
    new IASCanBuildFrom

  override def newBuilder[A]: mutable.Builder[A, ImmArraySeq[A]] =
    ImmArray.newBuilder.mapResult(_.toSeq)
}
