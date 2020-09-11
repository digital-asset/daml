// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import scala.util.Try
import scala.{concurrent => sc}

import scalaz.Id.Id

package object concurrent {

  /** Like [[scala.concurrent.Future]] but with an extra type parameter indicating
    * which [[ExecutionContext]] should be used for `map`, `flatMap` and other
    * operations.
    */
  type Future[-EC, +A] = FutureOf.Instance.T[EC, A]
  type ExecutionContext[+P] = ExecutionContextOf.Instance.T[P]
}

// keeping the companions with the same-named type aliases
package concurrent {

  object Future {

    /** {{{
      *  Future[MyECTag] { expr }
      * }}}
      *
      * returns `Future[MyECTag, E]` where `E` is `expr`'s inferred type and
      * `ExecutionContext[MyECTag]` is required implicitly.
      */
    def apply[EC]: apply[EC] =
      new apply(())

    final class apply[EC](private val ignore: Unit) extends AnyVal {
      def apply[A](body: => A)(implicit ec: ExecutionContext[EC]): Future[EC, A] =
        sc.Future(body)(ec)
    }

    def fromTry[EC, A](result: Try[A]): Future[EC, A] =
      sc.Future.fromTry(result)
  }

  object ExecutionContext {
    def apply[EC](ec: sc.ExecutionContext): ExecutionContext[EC] =
      ExecutionContextOf.Instance.subst[Id, EC](ec)
  }
}
