// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.Future

final class ReleasableResourceOwner[Context: HasExecutionContext, T](
    acquireSync: () => T
)(release: T => Future[Unit])
    extends AbstractResourceOwner[Context, T] {
  override def acquire()(implicit context: Context): Resource[Context, T] =
    ReleasableResource(Future.successful(acquireSync()))(release)
}
