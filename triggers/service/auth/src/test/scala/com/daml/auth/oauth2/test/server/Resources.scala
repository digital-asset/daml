// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.oauth2.test.server

import java.time.{Clock, Duration, Instant, ZoneId}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import com.daml.clock.AdjustableClock
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}

import scala.concurrent.Future

object Resources {
  def clock(start: Instant, zoneId: ZoneId): ResourceOwner[AdjustableClock] =
    new ResourceOwner[AdjustableClock] {
      override def acquire()(implicit context: ResourceContext): Resource[AdjustableClock] = {
        Resource(Future(AdjustableClock(Clock.fixed(start, zoneId), Duration.ZERO)))(_ =>
          Future(())
        )
      }
    }
  def authServerBinding(server: Server)(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        Resource(server.start())(_.unbind().map(_ => ()))
    }
  def authClientBinding(
      config: Client.Config
  )(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        Resource(Client.start(config))(_.unbind().map(_ => ()))
    }
}
