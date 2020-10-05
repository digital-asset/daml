// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.daml.ports.Port
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

// This is a test client (using terminology from oauth).
// The trigger service would also take the role of a client.
object Client {
  import com.daml.oauth.server.JsonProtocol._

  case class Config(
      port: Port,
      authServerUrl: Uri,
      clientId: String,
      clientSecret: String
  )

  object JsonProtocol extends DefaultJsonProtocol {
    implicit val accessParamsFormat: RootJsonFormat[AccessParams] = jsonFormat1(AccessParams)
    implicit val accessResponseFormat: RootJsonFormat[AccessResponse] = jsonFormat1(AccessResponse)
  }

  case class AccessParams(parties: Seq[String])
  case class AccessResponse(token: String)

  def toRedirectUri(uri: Uri): Uri = uri.withPath(Path./("cb"))

  def start(
      config: Config)(implicit asys: ActorSystem, ec: ExecutionContext): Future[ServerBinding] = {
    import JsonProtocol._
    val route = concat(
      // Some parameter that requires authorization for some parties. This will in the end return the token
      // produced by the authorization server.
      path("access") {
        post {
          entity(as[AccessParams]) {
            params =>
              extractRequest {
                request =>
                  val redirectUri = toRedirectUri(request.uri)
                  val authParams = Request.Authorize(
                    responseType = "code",
                    clientId = config.clientId,
                    redirectUri = redirectUri,
                    scope = Some(params.parties.map(p => "actAs:" + p).mkString(" ")),
                    state = None)
                  redirect(
                    config.authServerUrl
                      .withQuery(authParams.toQuery)
                      .withPath(Path./("authorize")),
                    StatusCodes.Found)
              }
          }
        }
      },
      path("cb") {
        get {
          parameters(('code, 'state ?)).as[Response.Authorize](Response.Authorize) {
            resp =>
              extractRequest {
                request =>
                  // We got the code, now request a token
                  val body = Request.Token(
                    grantType = "authorization_code",
                    code = resp.code,
                    redirectUri = toRedirectUri(request.uri),
                    clientId = config.clientId,
                    clientSecret = config.clientSecret)
                  val f = for {
                    entity <- Marshal(body).to[RequestEntity]
                    req = HttpRequest(
                      uri = config.authServerUrl.withPath(Path./("token")),
                      entity = entity,
                      method = HttpMethods.POST,
                    )
                    resp <- Http().singleRequest(req)
                    tokenResp <- Unmarshal(resp).to[Response.Token]
                  } yield tokenResp
                  onSuccess(f) { tokenResp =>
                    // Now we have the access_token and potentially the refresh token. At this point,
                    // we would start the trigger.
                    complete(AccessResponse(tokenResp.accessToken))
                  }
              }
          }
        }
      }
    )
    Http().bindAndHandle(route, "localhost", config.port.value)
  }

}
