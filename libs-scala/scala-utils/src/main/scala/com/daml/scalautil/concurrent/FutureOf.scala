// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil.concurrent

import scala.language.higherKinds
import scala.{concurrent => sc}
import scalaz.{Nondeterminism, Cobind, MonadError, Catchable}

sealed abstract class FutureOf {
  type T[-EC, +A]
  private[concurrent] def subst[F[_[+ _]], EC](ff: F[sc.Future]): F[T[EC, +?]]
}

object FutureOf {
  val Instance: FutureOf = new FutureOf {
    type T[-EC, +A] = sc.Future[A]
    override private[concurrent] def subst[F[_[+ _]], EC](ff: F[sc.Future]) = ff
  }

  type ScalazF[F[+ _]] = Nondeterminism[F]
    with Cobind[F]
    with MonadError[F, Throwable]
    with Catchable[F]

  implicit def `future Instance`[EC: ExecutionContext]: ScalazF[Future[EC, +?]] = {
    import scalaz.std.scalaFuture._
    Instance subst [ScalazF, EC] implicitly
  }

  /*
  implicit def `future Semigroup`[A: Semigroup, EC: ExecutionContext]: Semigroup[Future[EC, A]] = {
    import scalaz.std.scalaFuture._
    Instance subst [Semigroup, EC] implicitly
  }
 */
}
