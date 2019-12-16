// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.resources

import akka.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future}

class ActorSystemResourceOwner(acquireActorSystem: () => ActorSystem)
    extends ResourceOwner[ActorSystem] {
  override def acquire()(implicit _executionContext: ExecutionContext): Resource[ActorSystem] =
    new Resource[ActorSystem] {
      override protected val executionContext: ExecutionContext = _executionContext

      private val actorSystem: ActorSystem = acquireActorSystem()

      override protected val future: Future[ActorSystem] = Future.successful(actorSystem)

      override def releaseResource(): Future[Unit] = actorSystem.terminate().map(_ => ())
    }
}
