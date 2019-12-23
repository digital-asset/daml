// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import scala.concurrent.{ExecutionContext, Future}

class FutureResourceOwner[T](acquireFuture: () => Future[T]) extends ResourceOwner[T] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[T] =
    Resource(acquireFuture(), _ => Future.successful(()))
}
