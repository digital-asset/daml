// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources.akka

import akka.actor.ActorSystem
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

class ActorSystemResourceOwner(acquireActorSystem: () => ActorSystem)
    extends ResourceOwner[ActorSystem] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[ActorSystem] =
    Resource(Future(acquireActorSystem()))(_.terminate().map(_ => ()))
}
