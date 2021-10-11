// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import com.daml.scalautil.Statement.discard

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.generic.{
  CanBuildFrom,
  GenericCompanion,
  GenericTraversableTemplate,
  IndexedSeqFactory,
}
import scala.collection.{IndexedSeqLike, IndexedSeqOptimized, mutable}

import ImmArray.ImmArraySeq

abstract class AbstractImmArraySeq[+A](array: ImmArray[A])
    extends IndexedSeq[A]
    with GenericTraversableTemplate[A, ImmArraySeq]
    with IndexedSeqLike[A, ImmArraySeq[A]]
    with IndexedSeqOptimized[A, ImmArraySeq[A]] { this: ImmArraySeq[A] =>

  override final def companion: GenericCompanion[ImmArraySeq] = ImmArraySeq

  override final def copyToArray[B >: A](xs: Array[B], dstStart: Int, dstLen: Int): Unit = {
    discard(array.copyToArray(xs, dstStart, dstLen))
  }

  override final def map[B, That](
      f: A => B
  )(implicit bf: CanBuildFrom[ImmArraySeq[A], B, That]): That =
    bf match {
      case _: IASCanBuildFrom[B] => array.map(f).toSeq
      case _ => super.map(f)(bf)
    }

  override final def to[Col[_]](implicit
      bf: CanBuildFrom[Nothing, A, Col[A @uncheckedVariance]]
  ): Col[A @uncheckedVariance] =
    bf match {
      case _: IASCanBuildFrom[A] => this
      case _: ImmArrayInstances.IACanBuildFrom[A] => toImmArray
      case _: FrontStackInstances.FSCanBuildFrom[A] => FrontStack.from(toImmArray)
      case _ => super.to(bf)
    }
}

abstract class ImmArraySeqCompanion extends IndexedSeqFactory[ImmArraySeq] {
  this: ImmArraySeq.type =>

  protected type Factory[A] = CanBuildFrom[Coll, A, ImmArraySeq[A]]

  protected def canBuildFrom[A]: Factory[A] =
    new IASCanBuildFrom

  override final def newBuilder[A]: mutable.Builder[A, ImmArraySeq[A]] =
    ImmArray.newBuilder.mapResult(_.toSeq)
}

private[data] final class IASCanBuildFrom[A] extends ImmArraySeq.GenericCanBuildFrom[A]
