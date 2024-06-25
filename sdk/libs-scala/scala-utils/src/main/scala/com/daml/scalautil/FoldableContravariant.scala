// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import scalaz.{Monoid, Foldable, Semigroup}
import FoldableContravariant._

// Specialized overrides for when a Foldable wraps another Foldable.
// If you need to hand-write some of these, just mix in the traits you
// don't want to hand-write.
trait FoldableContravariant[X[_], Y[_]]
    extends CoreOps[X, Y]
    with Semigroupoids[X, Y]
    with Conversions[X, Y]
    with Lookups[X, Y]

object FoldableContravariant {
  trait CtMap[X[_], Y[_]] {
    protected[this] def Y: Foldable[Y]
    protected[this] def ctmap[A](ax: X[A]): Y[A]
  }

  // non-derived combinators
  trait CoreOps[X[_], Y[_]] extends Foldable[X] with CtMap[X, Y] {
    override final def foldLeft[A, B](xa: X[A], z: B)(f: (B, A) => B) = Y.foldLeft(ctmap(xa), z)(f)
    override final def foldRight[A, B](xa: X[A], z: => B)(f: (A, => B) => B) =
      Y.foldRight(ctmap(xa), z)(f)
    override final def foldMap[A, Z: Monoid](xa: X[A])(f: A => Z) = Y.foldMap(ctmap(xa))(f)
  }

  // plays on functions from the semigroupoids library
  trait Semigroupoids[X[_], Y[_]] extends Foldable[X] with CtMap[X, Y] {
    override final def foldMapRight1Opt[A, B](xa: X[A])(z: A => B)(f: (A, => B) => B) =
      Y.foldMapRight1Opt(ctmap(xa))(z)(f)
    override final def foldMapLeft1Opt[A, B](xa: X[A])(z: A => B)(f: (B, A) => B) =
      Y.foldMapLeft1Opt(ctmap(xa))(z)(f)
    override final def foldMap1Opt[A, B: Semigroup](xa: X[A])(f: A => B) =
      Y.foldMap1Opt(ctmap(xa))(f)
  }

  // to (collection type) converters
  trait Conversions[X[_], Y[_]] extends Foldable[X] with CtMap[X, Y] {
    override final def toStream[A](xa: X[A]) = Y.toStream(ctmap(xa))
    override final def toSet[A](xa: X[A]) = Y.toSet(ctmap(xa))
    override final def toList[A](xa: X[A]) = Y.toList(ctmap(xa))
    override final def toVector[A](xa: X[A]) = Y.toVector(ctmap(xa))
    override final def toIList[A](xa: X[A]) = Y.toIList(ctmap(xa))
    override final def toEphemeralStream[A](xa: X[A]) =
      Y.toEphemeralStream(ctmap(xa))
  }

  // list-like operations
  trait Lookups[X[_], Y[_]] extends Foldable[X] with CtMap[X, Y] {
    override final def index[A](xa: X[A], i: Int) = Y.index(ctmap(xa), i)
    override final def length[A](xa: X[A]) = Y.length(ctmap(xa))
    override final def all[A](xa: X[A])(p: A => Boolean): Boolean = Y.all(ctmap(xa))(p)
    override final def any[A](xa: X[A])(p: A => Boolean): Boolean = Y.any(ctmap(xa))(p)
  }
}
