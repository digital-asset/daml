// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.Future

class FutureCloseableResourceOwner[Context: HasExecutionContext, T <: AutoCloseable](
    acquireFutureCloseable: () => Future[T],
) extends AbstractResourceOwner[Context, T] {
  override def acquire()(implicit context: Context): Resource[Context, T] =
    Resource.apply(acquireFutureCloseable())(closeable => Future(closeable.close()))
}
