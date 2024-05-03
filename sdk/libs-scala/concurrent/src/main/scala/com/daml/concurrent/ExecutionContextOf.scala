// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.concurrent

import scala.{concurrent => sc}

sealed abstract class ExecutionContextOf {
  type T[+P] <: sc.ExecutionContext
  private[concurrent] def subst[F[_], P](fe: F[sc.ExecutionContext]): F[T[P]]
}

object ExecutionContextOf {
  val Instance: ExecutionContextOf = new ExecutionContextOf {
    type T[+P] = sc.ExecutionContext
    override private[concurrent] def subst[F[_], P](fe: F[sc.ExecutionContext]) = fe
  }
}
