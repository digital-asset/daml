// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import scala.concurrent.{ExecutionContext, Future}

class FutureCloseableResourceOwner[T <: AutoCloseable](acquireFutureCloseable: () => Future[T])
    extends ResourceOwner[T] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[T] =
    Resource(acquireFutureCloseable(), closeable => Future(closeable.close()))
}
