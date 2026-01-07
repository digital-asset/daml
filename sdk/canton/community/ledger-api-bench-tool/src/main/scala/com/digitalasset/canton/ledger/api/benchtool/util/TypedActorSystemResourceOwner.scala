// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.util

import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.resources.{AbstractResourceOwner, ReleasableResource, Resource}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, Behavior, SpawnProtocol}

import scala.concurrent.Future

class TypedActorSystemResourceOwner[BehaviorType](
    acquireActorSystem: () => ActorSystem[BehaviorType]
) extends AbstractResourceOwner[ResourceContext, ActorSystem[BehaviorType]] {
  override def acquire()(implicit
      context: ResourceContext
  ): Resource[ResourceContext, ActorSystem[BehaviorType]] =
    ReleasableResource(Future(acquireActorSystem()))(system => Future(system.terminate()))
}

object TypedActorSystemResourceOwner {
  def owner(): ResourceOwner[ActorSystem[SpawnProtocol.Command]] =
    new TypedActorSystemResourceOwner[SpawnProtocol.Command](() =>
      ActorSystem(Creator(), "Creator")
    )

  object Creator {
    def apply(): Behavior[SpawnProtocol.Command] =
      Behaviors.setup { context =>
        context.log.debug(s"Starting Creator actor")
        SpawnProtocol()
      }
  }
}
