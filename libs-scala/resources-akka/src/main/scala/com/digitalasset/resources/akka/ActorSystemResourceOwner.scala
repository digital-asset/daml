// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.resources.akka

import akka.actor.ActorSystem
import com.daml.resources.{AbstractResourceOwner, HasExecutionContext, Resource}

import scala.concurrent.Future

class ActorSystemResourceOwner[Context: HasExecutionContext](acquireActorSystem: () => ActorSystem)
    extends AbstractResourceOwner[Context, ActorSystem] {
  override def acquire()(implicit context: Context): Resource[Context, ActorSystem] =
    Resource[Context].apply(Future(acquireActorSystem()))(_.terminate().map(_ => ()))
}
