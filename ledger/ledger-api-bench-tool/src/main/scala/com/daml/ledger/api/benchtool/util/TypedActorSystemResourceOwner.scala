// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.util

import akka.actor.typed.ActorSystem
import com.daml.ledger.resources.ResourceContext
import com.daml.resources.{AbstractResourceOwner, ReleasableResource, Resource}

import scala.concurrent.Future

class TypedActorSystemResourceOwner[BehaviorType](
    acquireActorSystem: () => ActorSystem[BehaviorType]
) extends AbstractResourceOwner[ResourceContext, ActorSystem[BehaviorType]] {
  override def acquire()(implicit
      context: ResourceContext
  ): Resource[ResourceContext, ActorSystem[BehaviorType]] =
    ReleasableResource(Future(acquireActorSystem()))(system => Future(system.terminate()))
}
