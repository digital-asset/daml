// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.{Http, HttpExt}
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpMethods,
  HttpRequest,
  Uri
}
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials, OAuth2BearerToken}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.Materializer
import spray.json._

import scala.concurrent.{ExecutionContext, Future}

object AuthServiceDomain extends DefaultJsonProtocol {
  // OAuth2 bearer token for requests to authentication service
  case class AuthServiceToken(token: String)
  implicit val authServiceTokenFormat: RootJsonFormat[AuthServiceToken] = jsonFormat1(
    AuthServiceToken)

  case class Nonce(nonce: String)
  implicit val nonceFormat: RootJsonFormat[Nonce] = jsonFormat1(Nonce)
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

  def requestServiceAccount(authServiceToken: AuthServiceToken, ledgerId: String): Future[Boolean] = {
    val uri = authServiceBaseUri.withPath(Path(s"/sa/secure/request/$ledgerId"))
    val authHeader = Authorization(OAuth2BearerToken(authServiceToken.token))
    val nonce = Nonce(UUID.randomUUID.toString)
    val entity = HttpEntity(ContentTypes.`application/json`, nonce.toJson.toString)
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri,
      headers = List(authHeader),
      entity
    )
    http.singleRequest(req).map(_.status.isSuccess)
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
