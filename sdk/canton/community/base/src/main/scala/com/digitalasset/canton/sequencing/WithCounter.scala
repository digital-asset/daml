// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.{Applicative, Eval, Functor, Traverse}
import com.digitalasset.canton.SequencerCounter

import scala.language.implicitConversions

final case class WithCounter[+WrappedElement](counter: SequencerCounter, element: WrappedElement) {
  def traverse[F[_], B](f: WrappedElement => F[B])(implicit F: Functor[F]): F[WithCounter[B]] =
    F.map(f(element))(WithCounter(counter, _))
}

object WithCounter {
  implicit def asElement[WrappedElement](withCounter: WithCounter[WrappedElement]): WrappedElement =
    withCounter.element

  implicit val traverseWithCounter: Traverse[WithCounter] = new Traverse[WithCounter] {
    override def traverse[G[_]: Applicative, A, B](withCounter: WithCounter[A])(
        f: A => G[B]
    ): G[WithCounter[B]] =
      withCounter.traverse(f)

    override def foldLeft[A, B](withCounter: WithCounter[A], b: B)(f: (B, A) => B): B =
      f(b, withCounter.element)

    override def foldRight[A, B](withCounter: WithCounter[A], lb: Eval[B])(
        f: (A, Eval[B]) => Eval[B]
    ): Eval[B] =
      f(withCounter.element, lb)
  }
}
