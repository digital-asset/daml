// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.Future

class CloseableResourceOwner[Context: HasExecutionContext, T <: AutoCloseable](
    acquireCloseable: () => T
) extends AbstractResourceOwner[Context, T] {
  override def acquire()(implicit context: Context): Resource[Context, T] =
    ReleasableResource(Future(acquireCloseable()))(closeable => Future(closeable.close()))
}
