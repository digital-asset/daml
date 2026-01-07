// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import scala.util.Try

trait WithExecuteUnsafe {
  def executeUnsafe[T](f: => T, symbol: Symbol): Either[Error, T] =
    Try(f).toEither.left.map(e => Error(symbol, e.getMessage))
}
