// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.pekko

import org.apache.pekko.actor.Cancellable
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future

class CancellableResourceOwner[C <: Cancellable, Context: HasExecutionContext](
    acquireCancellable: () => C
) extends AbstractResourceOwner[Context, C] {
  override def acquire()(implicit context: Context): Resource[Context, C] =
    ReleasableResource(Future(acquireCancellable()))(cancellable =>
      Future(cancellable.cancel()).map(_ => ())
    )
}
