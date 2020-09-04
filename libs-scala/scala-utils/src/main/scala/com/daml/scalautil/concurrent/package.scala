// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

package object concurrent {

  /** Like [[scala.concurrent.Future]] but with an extra type parameter indicating
    * which [[ExecutionContext]] should be used for `map`, `flatMap` and other
    * operations.
    */
  type Future[-EC, +A] = FutureOf.Instance.T[EC, A]
  type ExecutionContext[+P] = ExecutionContextOf.Instance.T[P]
}
