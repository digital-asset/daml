// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

import java.io.File
import java.nio.file.Files
import java.time.{Clock, Duration, Instant, ZoneId}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import com.daml.auth.middleware.api.Client
import com.daml.auth.middleware.api.Request.Claims
import com.daml.clock.AdjustableClock
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.auth.oauth2.test.server.{Server => OAuthServer}
import com.daml.scalautil.Statement.discard

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
  def temporaryDirectory(): ResourceOwner[File] =
    new ResourceOwner[File] {
      override def acquire()(implicit context: ResourceContext): Resource[File] =
        Resource(Future(Files.createTempDirectory("daml-oauth2-middleware").toFile))(dir =>
          Future(discard { dir.delete() })
        )
    }
  def authServerBinding(
      server: OAuthServer
  )(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        Resource(server.start())(_.unbind().map(_ => ()))
    }
  def authMiddlewareBinding(
      config: Config
  )(implicit sys: ActorSystem): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        Resource(Server.start(config))(_.unbind().map(_ => ()))
    }
  def authMiddlewareClientBinding(client: Client, callbackPath: Uri.Path)(implicit
      sys: ActorSystem
  ): ResourceOwner[ServerBinding] =
    new ResourceOwner[ServerBinding] {
      override def acquire()(implicit context: ResourceContext): Resource[ServerBinding] =
        Resource {
          Http()
            .newServerAt("localhost", 0)
            .bind {
              extractUri { reqUri =>
                val callbackUri = Uri()
                  .withScheme(reqUri.scheme)
                  .withAuthority(reqUri.authority)
                  .withPath(callbackPath)
                val clientRoutes = client.routes(callbackUri)
                concat(
                  path("authorize") {
                    get {
                      parameters(Symbol("claims").as[Claims]) { claims =>
                        clientRoutes.authorize(claims) {
                          case Client.Authorized(authorization) =>
                            import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
                            import com.daml.auth.middleware.api.JsonProtocol.responseAuthorizeFormat
                            complete(StatusCodes.OK, authorization)
                          case Client.Unauthorized =>
                            complete(StatusCodes.Unauthorized)
                          case Client.LoginFailed(loginError) =>
                            import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
                            import com.daml.auth.middleware.api.JsonProtocol.ResponseLoginFormat
                            complete(
                              StatusCodes.Forbidden,
                              loginError: com.daml.auth.middleware.api.Response.Login,
                            )
                        }
                      }
                    }
                  },
                  path("login") {
                    get {
                      parameters(Symbol("claims").as[Claims]) { claims =>
                        import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
                        import com.daml.auth.middleware.api.JsonProtocol.ResponseLoginFormat
                        clientRoutes.login(claims, login => complete(StatusCodes.OK, login))
                      }
                    }
                  },
                  path("cb") { get { clientRoutes.callbackHandler } },
                )
              }
            }
        } {
          _.unbind().map(_ => ())
        }
    }
}
