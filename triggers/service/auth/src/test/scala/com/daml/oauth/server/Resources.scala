// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.ExecutionContext

object Resources {
  def authServer(config: Config)(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit executionContext: ExecutionContext): Resource[ServerBinding] =
        Resource(Server.start(config))(_.unbind().map(_ => ()))
    }
  def authClient(config: Client.Config)(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit ec: ExecutionContext): Resource[ServerBinding] =
        Resource(Client.start(config))(_.unbind().map(_ => ()))
    }
}
