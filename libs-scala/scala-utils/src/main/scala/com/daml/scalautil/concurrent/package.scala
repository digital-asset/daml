// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

package object concurrent {
  type Future[-EC, +A] = FutureOf.Instance.T[EC, A]
  type ExecutionContext[+P] = ExecutionContextOf.Instance.T[P]
}
