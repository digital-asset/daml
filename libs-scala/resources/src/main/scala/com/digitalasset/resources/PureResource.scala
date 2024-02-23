// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources

import scala.concurrent.Future

/** Represents a pure success or failure as a resource. */
private[resources] final class PureResource[Context: HasExecutionContext, T] private (
    future: Future[T]
) extends Resource[Context, T] {
  override def asFuture: Future[T] = future

  override def release(): Future[Unit] = Future.unit
}

object PureResource {
  def apply[Context: HasExecutionContext, T](future: Future[T]): Resource[Context, T] =
    new PureResource(future)
}
