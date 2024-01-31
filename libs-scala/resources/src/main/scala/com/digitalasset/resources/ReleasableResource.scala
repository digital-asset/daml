// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.Future

object ReleasableResource {
  def apply[Context: HasExecutionContext, T](
      future: Future[T]
  )(releaseResource: T => Future[Unit])(implicit context: Context): Resource[Context, T] =
    NestedResource(future)(releaseResource, () => Future.unit)
}
