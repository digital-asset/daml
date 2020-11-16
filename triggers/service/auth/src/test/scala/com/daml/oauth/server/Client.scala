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
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
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
    implicit object ResponseJsonFormat extends RootJsonFormat[Response] {
      implicit private val accessFormat: RootJsonFormat[AccessResponse] = jsonFormat2(
        AccessResponse)
      implicit private val errorFormat: RootJsonFormat[ErrorResponse] = jsonFormat1(ErrorResponse)
      def write(resp: Response) = resp match {
        case resp @ AccessResponse(_, _) => resp.toJson
        case resp @ ErrorResponse(_) => resp.toJson
      }
      def read(value: JsValue) =
        (value.convertTo(safeReader[AccessResponse]), value.convertTo(safeReader[ErrorResponse])) match {
          case (Right(a), _) => a
          case (_, Right(b)) => b
          case (Left(ea), Left(eb)) =>
            deserializationError(s"Could not read Response value:\n$ea\n$eb")
        }
    }
  }

  case class AccessParams(parties: Seq[String])
  sealed trait Response
  final case class AccessResponse(token: String, refresh: String) extends Response
  final case class ErrorResponse(error: String) extends Response

  def toRedirectUri(uri: Uri): Uri = uri.withPath(Path./("cb"))

  def start(
      config: Config)(implicit asys: ActorSystem, ec: ExecutionContext): Future[ServerBinding] = {
    import JsonProtocol._
    implicit val unmarshal: Unmarshaller[String, Uri] = Unmarshaller.strict(Uri(_))
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
                    state = None,
                    audience = Some("https://daml.com/ledger-api")
                  )
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
                    complete(
                      AccessResponse(tokenResp.accessToken, tokenResp.refreshToken.get): Response)
                  }
              }
          } ~
            parameters(('error, 'error_description ?, 'error_uri.as[Uri] ?, 'state ?))
              .as[Response.Error](Response.Error) { resp =>
                complete(ErrorResponse(resp.error): Response)
              }
        }
      }
    )
    Http().bindAndHandle(route, "localhost", config.port.value)
  }

}
