// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.concurrent

import scala.language.{higherKinds, implicitConversions}
import scala.{concurrent => sc}
import scala.util.Try

import scalaz.{Catchable, Cobind, Isomorphism, Leibniz, MonadError, Nondeterminism, Semigroup}
import Isomorphism.<~>
import Leibniz.===
import scalaz.std.scalaFuture._

sealed abstract class FutureOf {

  /** We don't use [[sc.Future]] as the upper bound because it has methods that
    * collide with the versions we want to use, i.e. those that preserve the
    * phantom `EC` type parameter.  By contrast, [[sc.Awaitable]] has only the
    * `ready` and `result` methods, which are mostly useless.
    */
  type T[-EC, +A] <: sc.Awaitable[A]
  private[concurrent] def subst[F[_[+ _]], EC](ff: F[sc.Future]): F[T[EC, +?]]
}

/** Instances and methods for `FutureOf`. You should not import these; instead,
  * enable `-Xsource:2.13` and they will always be available without import.
  */
object FutureOf {
  val Instance: FutureOf = new FutureOf {
    type T[-EC, +A] = sc.Future[A]
    override private[concurrent] def subst[F[_[+ _]], EC](ff: F[sc.Future]) = ff
  }

  type ScalazF[F[+ _]] = Nondeterminism[F]
    with Cobind[F]
    with MonadError[F, Throwable]
    with Catchable[F]

  implicit def `future Instance`[EC: ExecutionContext]: ScalazF[Future[EC, +?]] =
    Instance subst [ScalazF, EC] implicitly

  implicit def `future Semigroup`[A: Semigroup, EC: ExecutionContext]: Semigroup[Future[EC, A]] = {
    type K[T[+ _]] = Semigroup[T[A]]
    Instance subst [K, EC] implicitly
  }

  implicit def `future is any type`[A]: sc.Future[A] === Future[Any, A] =
    Instance subst [Lambda[`t[+_]` => sc.Future[A] === t[A]], Any] Leibniz.refl

  /** A [[sc.Future]] converts to our [[Future]] with any choice of EC type. */
  implicit def `future is any`[A](sf: sc.Future[A]): Future[Any, A] =
    `future is any type`(sf)

  def swapExecutionContext[L, R]: Future[L, ?] <~> Future[R, ?] =
    Instance.subst[Lambda[`t[+_]` => t <~> Future[R, ?]], L](
      Instance.subst[Lambda[`t[+_]` => sc.Future <~> t], R](implicitly[sc.Future <~> sc.Future]))

  /** Common methods like `map` and `flatMap` are not provided directly; instead,
    * import the appropriate Scalaz syntax for these; `scalaz.syntax.bind._`
    * will give you `map`, `flatMap`, and most other common choices.  Only
    * exotic Future-specific combinators are provided here.
    */
  implicit final class Ops[EC, A](private val self: Future[EC, A]) extends AnyVal {
    def collect[B](pf: A PartialFunction B)(implicit ec: ExecutionContext[EC]): Future[EC, B] =
      self.removeExecutionContext collect pf

    def fallbackTo[LEC <: EC, B >: A](that: Future[LEC, B]): Future[LEC, B] =
      self.removeExecutionContext fallbackTo that.removeExecutionContext

    def filter(p: A => Boolean)(implicit ec: ExecutionContext[EC]): Future[EC, A] =
      self.removeExecutionContext filter p

    def andThen[U](pf: Try[A] PartialFunction U)(implicit ec: ExecutionContext[EC]): Future[EC, A] =
      self.removeExecutionContext andThen pf

    def onComplete[U](f: Try[A] => U)(implicit ec: ExecutionContext[EC]): Unit =
      self.removeExecutionContext onComplete f
  }

  /** Operations that don't refer to an ExecutionContext. */
  implicit final class NonEcOps[A](private val self: Future[Nothing, A]) extends AnyVal {

    /** Switch execution contexts for later operations.  This is not necessary if
      * `NEC <: EC`, as the future will simply widen in those cases.
      */
    def changeExecutionContext[NEC]: Future[NEC, A] =
      swapExecutionContext[Nothing, NEC].to(self)

    /** The "unsafe" conversion to Future.  Does nothing itself, but removes
      * the control on which [[sc.ExecutionContext]] is used for later
      * operations.
      */
    def removeExecutionContext: sc.Future[A] =
      self.changeExecutionContext[Any].asScala

    def value: Option[Try[A]] = self.removeExecutionContext.value
  }

  /** Operations safe if the Future is set to any ExecutionContext. */
  implicit final class AnyOps[+A](private val self: Future[Any, A]) extends AnyVal {

    /** The "safe" conversion to Future.  `EC = Any` already means "use any
      * ExecutionContext", so there is little harm in restating that by
      * referring directly to [[sc.Future]].
      */
    def asScala: sc.Future[A] = `future is any type`[A].flip(self)
  }
}
