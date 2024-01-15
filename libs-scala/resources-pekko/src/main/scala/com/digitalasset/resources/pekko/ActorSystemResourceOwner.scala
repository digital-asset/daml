// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.pekko

import org.apache.pekko.actor.ActorSystem
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, ReleasableResource, Resource}

import scala.concurrent.Future

class ActorSystemResourceOwner[Context: HasExecutionContext](acquireActorSystem: () => ActorSystem)
    extends AbstractResourceOwner[Context, ActorSystem] {
  override def acquire()(implicit context: Context): Resource[Context, ActorSystem] =
    ReleasableResource(Future(acquireActorSystem()))(_.terminate().map(_ => ()))
}
