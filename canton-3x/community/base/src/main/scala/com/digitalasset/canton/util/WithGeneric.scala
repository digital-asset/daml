// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.{Applicative, Eval, Functor}

/** Generic implementation for creating a container of single `A`s paired with a value of type `B`
  * with appropriate `map` and `traverse` implementations.
  */
trait WithGeneric[+A, B, C[+_]] {
  protected def unwrap: A
  protected def added: B
  protected def update[AA](newValue: AA): C[AA]

  // Copies of the above protected methods
  // so that we can access them from the companion object without having to make them widely visible
  private[util] def unwrapInternal: A = unwrap
  private[util] def addedInternal: B = added
  private[util] def updateInternal[AA](newValue: AA): C[AA] = update(newValue)

  def map[AA](f: A => AA): C[AA] = update(f(unwrap))
  def traverse[F[_], AA](f: A => F[AA])(implicit F: Functor[F]): F[C[AA]] =
    F.map(f(unwrap))(updateInternal)
}

trait WithGenericCompanion {
  def singletonTraverseWithGeneric[B, X[+A] <: WithGeneric[A, B, X]]: SingletonTraverse.Aux[X, B] =
    new SingletonTraverse[X] {
      override type Context = B

      override def traverseSingleton[G[_], A, AA](x: X[A])(f: (B, A) => G[AA])(implicit
          G: Applicative[G]
      ): G[X[AA]] =
        G.map(f(x.addedInternal, x.unwrapInternal))(x.updateInternal)

      override def traverse[G[_], A, AA](fa: X[A])(f: A => G[AA])(implicit
          G: Applicative[G]
      ): G[X[AA]] = fa.traverse(f)

      override def foldLeft[A, S](fa: X[A], s: S)(f: (S, A) => S): S = f(s, fa.unwrapInternal)

      override def foldRight[A, S](fa: X[A], ls: Eval[S])(f: (A, Eval[S]) => Eval[S]): Eval[S] =
        f(fa.unwrapInternal, ls)
    }
}
