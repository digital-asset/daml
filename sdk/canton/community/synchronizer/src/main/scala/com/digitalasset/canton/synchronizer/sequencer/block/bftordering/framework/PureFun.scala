// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework

/** A subset of functions that are pure, this is used by [[ModuleContext.mapFuture]] to guarantee we don't mutate state
  * when we shouldn't.
  *
  * This trait is sealed so that we *can't* use lambda abstraction to construct values of this type
  *
  * @tparam A input type
  * @tparam B output type
  */
sealed trait PureFun[A, B] extends (A => B) {
  final def andThen[C](g: PureFun[B, C]): PureFun[A, C] = PureFun.Compose(this, g)
}

object PureFun {

  private final case class Compose[A, B, C](f1: PureFun[A, B], f2: PureFun[B, C])
      extends PureFun[A, C] {
    override def apply(v1: A): C = f2(f1(v1))
  }

  object Discard extends PureFun[Any, Unit] {
    override def apply(v1: Any): Unit = ()
  }

  final case class Const[A](v: A) extends PureFun[Unit, A] {
    override def apply(v1: Unit): A = v
  }

  object Seq {
    final case class Flatten[A]() extends PureFun[Seq[Seq[A]], Seq[A]] {
      override def apply(v1: Seq[Seq[A]]): Seq[A] = v1.flatten
    }
  }

  object Either {
    final case class LeftMap[A, B, C](fun: PureFun[A, B])
        extends PureFun[Either[A, C], Either[B, C]] {
      override def apply(v1: Either[A, C]): Either[B, C] = v1.left.map(fun)
    }
  }

  object Util {
    final case class CollectPairErrors[E]()
        extends PureFun[(Either[Seq[E], Unit], Either[Seq[E], Unit]), Either[Seq[E], Unit]] {
      override def apply(pair: (Either[Seq[E], Unit], Either[Seq[E], Unit])): Either[Seq[E], Unit] =
        pair match {
          case (Left(l), Left(r)) =>
            Left(l ++ r)
          case (Left(l), Right(())) =>
            Left(l)
          case (Right(()), Left(r)) =>
            Left(r)
          case (Right(()), Right(())) =>
            Right(())
        }
    }

    final case class CollectErrors[E]()
        extends PureFun[Seq[Either[E, Unit]], Either[Seq[E], Unit]] {
      override def apply(xs: Seq[Either[E, Unit]]): Either[Seq[E], Unit] = {
        val (left, _) = xs.partitionMap(identity)

        if (left.isEmpty) {
          Right(())
        } else {
          Left(left)
        }
      }
    }
  }
}
