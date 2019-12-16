// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import scala.concurrent.{ExecutionContext, Future}

class FutureResourceOwner[T](acquireFuture: () => Future[T]) extends ResourceOwner[T] {
  override def acquire()(implicit _executionContext: ExecutionContext): Resource[T] =
    new Resource[T] {
      override protected val executionContext: ExecutionContext = _executionContext

      override protected val future: Future[T] = acquireFuture()

      override def release(): Future[Unit] = Future.successful(())
    }
}
