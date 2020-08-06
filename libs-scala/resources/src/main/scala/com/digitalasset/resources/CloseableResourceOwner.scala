// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.Future

class CloseableResourceOwner[Context: HasExecutionContext, T <: AutoCloseable](
    acquireCloseable: () => T,
) extends AbstractResourceOwner[Context, T] {
  override def acquire()(implicit context: Context): Resource[T] =
    Resource(Future(acquireCloseable()))(closeable => Future(closeable.close()))
}
