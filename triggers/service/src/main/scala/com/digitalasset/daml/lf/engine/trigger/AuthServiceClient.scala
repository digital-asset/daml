// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

object AuthServiceDomain extends DefaultJsonProtocol {
  // OAuth2 bearer token for requests to authentication service
  case class AuthServiceToken(token: String)
  implicit val authServiceTokenFormat: RootJsonFormat[AuthServiceToken] = jsonFormat1(
    AuthServiceToken)
}

class AuthServiceClient(authServiceBaseUri: Uri)(
    implicit system: ActorSystem,
    materializer: Materializer,
    ec: ExecutionContext) {
  import AuthServiceDomain._

  private val http: HttpExt = Http(system)

  def authorize(username: String, password: String): Future[AuthServiceToken] = {
    val authorizeUri = authServiceBaseUri.withPath(Path("/sa/secure/authorize"))
    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = authorizeUri,
      headers = List(Authorization(BasicHttpCredentials(username, password)))
    )
    for {
      authResponse <- http.singleRequest(request)
      authServiceToken <- Unmarshal(authResponse).to[AuthServiceToken]
    } yield authServiceToken
  }

}

object AuthServiceClient {
  def apply(authServiceBaseUri: Uri)(
      implicit system: ActorSystem,
      materializer: Materializer,
      ec: ExecutionContext,
  ): AuthServiceClient = {
    new AuthServiceClient(authServiceBaseUri)
  }
}
