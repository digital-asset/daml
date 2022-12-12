// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.scalautil

import com.daml.{nonempty => dne}

package object nonempty {
  @deprecated("use com.daml.nonempty.NonEmpty instead", since = "2.1.0")
  val NonEmpty: dne.NonEmpty.type = dne.NonEmpty

  @deprecated("use com.daml.nonempty.NonEmpty instead", since = "2.1.0")
  type NonEmpty[+A] = dne.NonEmpty[A]

  @deprecated("use com.daml.nonempty.NonEmptyF instead", since = "2.1.0")
  type NonEmptyF[F[_], A] = dne.NonEmptyF[F, A]

  @deprecated("use com.daml.nonempty.NonEmptyReturningOps instead", since = "2.1.0")
  val NonEmptyReturningOps: dne.NonEmptyReturningOps.type = dne.NonEmptyReturningOps
}
