// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import scala.concurrent.ExecutionContext

trait ResourceOwner[T] {
  def acquire()(implicit _executionContext: ExecutionContext): Resource[T]
}

object ResourceOwner {
  def apply[T <: AutoCloseable](acquireCloseable: () => T): ResourceOwner[T] =
    new AutoCloseableResourceOwner[T](acquireCloseable)
}
