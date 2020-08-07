// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.oauth.server.{Config => OAuthConfig, Server => OAuthServer}
import com.daml.resources.Resource

object Resources {
  def authServer(config: OAuthConfig)(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        Resource(OAuthServer.start(config))(_.unbind().map(_ => ()))
    }
  def authMiddleware(config: Config)(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        Resource(Server.start(config))(_.unbind().map(_ => ()))
    }
}
