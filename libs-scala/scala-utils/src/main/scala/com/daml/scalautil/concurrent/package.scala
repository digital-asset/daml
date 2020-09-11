// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import scala.util.Try

package object concurrent {

  /** Like [[scala.concurrent.Future]] but with an extra type parameter indicating
    * which [[ExecutionContext]] should be used for `map`, `flatMap` and other
    * operations.
    */
  type Future[-EC, +A] = FutureOf.Instance.T[EC, A]
  type ExecutionContext[+P] = ExecutionContextOf.Instance.T[P]

  object Future {
    def apply[EC]: apply[EC] =
      new apply(())

    final class apply[EC](private val ignore: Unit) extends AnyVal {
      def apply[A](body: => A)(implicit ec: ExecutionContext[EC]): Future[EC, A] =
        scala.concurrent.Future(body)(ec)
    }

    def fromTry[EC, A](result: Try[A]): Future[EC, A] =
      scala.concurrent.Future.fromTry(result)
  }

  object ExecutionContext {
    def apply[EC](ec: scala.concurrent.ExecutionContext): ExecutionContext[EC] =
      ExecutionContextOf.Instance.subst[scalaz.Id.Id, EC](ec)
  }
}
