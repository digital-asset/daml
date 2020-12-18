// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.collection.StrictOptimizedSeqFactory
import scala.collection.immutable.{AbstractSeq, IndexedSeqOps, StrictOptimizedSeqOps}

import ImmArray.ImmArraySeq

abstract class AbstractImmArraySeq[+A](array: ImmArray[A])
    extends AbstractSeq[A]
    with IndexedSeq[A]
    with IndexedSeqOps[A, ImmArraySeq, ImmArraySeq[A]]
    with StrictOptimizedSeqOps[A, ImmArraySeq, ImmArraySeq[A]]
    with scala.collection.IterableFactoryDefaults[A, ImmArraySeq] { this: ImmArraySeq[A] =>

  override final def iterableFactory: scala.collection.SeqFactory[ImmArraySeq] = ImmArraySeq

  override final def copyToArray[B >: A](xs: Array[B], dstStart: Int, dstLen: Int): Int =
    array.copyToArray(xs, dstStart, dstLen)
}

abstract class ImmArraySeqCompanion extends StrictOptimizedSeqFactory[ImmArraySeq] {
  this: ImmArraySeq.type =>
  protected type Factory[A] = Unit
  protected def canBuildFrom[A]: Factory[A] = ()
  final def empty[A] = ImmArray.empty.toSeq
  final def from[E](it: IterableOnce[E]) = (ImmArray.newBuilder ++= it).result().toSeq
  final def newBuilder[A] = ImmArray.newBuilder.mapResult(_.toSeq)
}
