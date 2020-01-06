// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import akka.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

class ActorSystemResourceOwner(acquireActorSystem: () => ActorSystem)
    extends ResourceOwner[ActorSystem] {
  override def acquire()(implicit executionContext: ExecutionContext): Resource[ActorSystem] =
    Resource(Future(acquireActorSystem()), actorSystem => actorSystem.terminate().map(_ => ()))
}
