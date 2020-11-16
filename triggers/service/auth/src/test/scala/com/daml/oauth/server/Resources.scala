// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.server

import java.time.{Clock, Instant, ZoneId}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}

import scala.concurrent.Future

object Resources {
  def clock(start: Instant, zoneId: ZoneId): ResourceOwner[Clock] =
    new ResourceOwner[Clock] {
      override def acquire()(implicit context: ResourceContext): Resource[Clock] = {
        Resource(Future(Clock.fixed(start, zoneId)))(_ => Future(()))
      }
    }
  def authServer(config: Config)(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        Resource(Server(config).start())(_.unbind().map(_ => ()))
    }
  def authClient(config: Client.Config)(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        Resource(Client.start(config))(_.unbind().map(_ => ()))
    }
}
