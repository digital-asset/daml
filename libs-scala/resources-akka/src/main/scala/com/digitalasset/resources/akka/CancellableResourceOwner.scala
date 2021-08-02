// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.actor.Cancellable
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future

class CancellableResourceOwner[Context: HasExecutionContext](acquireCancellable: () => Cancellable)
    extends AbstractResourceOwner[Context, Cancellable] {
  override def acquire()(implicit context: Context): Resource[Context, Cancellable] =
    ReleasableResource(Future(acquireCancellable()))(cancellable =>
      Future(cancellable.cancel()).map(_ => ())
    )
}
