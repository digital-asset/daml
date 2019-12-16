// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import scala.concurrent.{ExecutionContext, Future}

class FutureCloseableResourceOwner[T <: AutoCloseable](acquireFutureCloseable: () => Future[T])
    extends ResourceOwner[T] {
  override def acquire()(implicit _executionContext: ExecutionContext): Resource[T] =
    new Resource[T] {
      override protected val executionContext: ExecutionContext = _executionContext

      override protected val future: Future[T] = acquireFutureCloseable()

      override def releaseResource(): Future[Unit] = asFuture.map(_.close())
    }
}
