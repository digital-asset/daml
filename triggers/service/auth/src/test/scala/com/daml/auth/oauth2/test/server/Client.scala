// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.oauth2.test.server

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.Uri.Path
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import com.daml.auth.oauth2.api.{Request, Response}
import com.daml.ports.Port
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

// This is a test client (using terminology from oauth).
// The trigger service would also take the role of a client.
object Client {
  import com.daml.auth.oauth2.api.JsonProtocol._

  case class Config(
      port: Port,
      authServerUrl: Uri,
      clientId: String,
      clientSecret: String,
  )

  object JsonProtocol extends DefaultJsonProtocol {
    implicit val accessParamsFormat: RootJsonFormat[AccessParams] = jsonFormat3(AccessParams)
    implicit val refreshParamsFormat: RootJsonFormat[RefreshParams] = jsonFormat1(RefreshParams)
    implicit object ResponseJsonFormat extends RootJsonFormat[Response] {
      implicit private val accessFormat: RootJsonFormat[AccessResponse] = jsonFormat2(
        AccessResponse
      )
      implicit private val errorFormat: RootJsonFormat[ErrorResponse] = jsonFormat1(ErrorResponse)
      def write(resp: Response) = resp match {
        case resp @ AccessResponse(_, _) => resp.toJson
        case resp @ ErrorResponse(_) => resp.toJson
      }
      def read(value: JsValue) =
        (
          value.convertTo(safeReader[AccessResponse]),
          value.convertTo(safeReader[ErrorResponse]),
        ) match {
          case (Right(a), _) => a
          case (_, Right(b)) => b
          case (Left(ea), Left(eb)) =>
            deserializationError(s"Could not read Response value:\n$ea\n$eb")
        }
    }
  }

  case class AccessParams(parties: Seq[String], admin: Boolean, applicationId: Option[String])
  case class RefreshParams(refreshToken: String)
  sealed trait Response
  final case class AccessResponse(token: String, refresh: String) extends Response
  final case class ErrorResponse(error: String) extends Response

  def toRedirectUri(uri: Uri): Uri = uri.withPath(Path./("cb"))

  def start(
      config: Config
  )(implicit asys: ActorSystem, ec: ExecutionContext): Future[ServerBinding] = {
    import JsonProtocol._
    implicit val unmarshal: Unmarshaller[String, Uri] = Unmarshaller.strict(Uri(_))
    val route = concat(
      // Some parameter that requires authorization for some parties. This will in the end return the token
      // produced by the authorization server.
      path("access") {
        post {
          entity(as[AccessParams]) { params =>
            extractRequest { request =>
              val redirectUri = toRedirectUri(request.uri)
              val scope =
                (params.parties.map(p => "actAs:" + p) ++
                  (if (params.admin) List("admin") else Nil) ++
                  params.applicationId.toList.map(id => "applicationId:" + id)).mkString(" ")
              val authParams = Request.Authorize(
                responseType = "code",
                clientId = config.clientId,
                redirectUri = redirectUri,
                scope = Some(scope),
                state = None,
                audience = Some("https://daml.com/ledger-api"),
              )
              redirect(
                config.authServerUrl
                  .withQuery(authParams.toQuery)
                  .withPath(Path./("authorize")),
                StatusCodes.Found,
              )
            }
          }
        }
      },
      path("cb") {
        get {
          parameters(Symbol("code"), Symbol("state") ?).as[Response.Authorize](Response.Authorize) {
            resp =>
              extractRequest { request =>
                // We got the code, now request a token
                val body = Request.Token(
                  grantType = "authorization_code",
                  code = resp.code,
                  redirectUri = toRedirectUri(request.uri),
                  clientId = config.clientId,
                  clientSecret = config.clientSecret,
                )
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
                    AccessResponse(
                      tokenResp.accessToken,
                      tokenResp.refreshToken.getOrElse(
                        sys.error("/token endpoint failed to return a refresh token")
                      ),
                    ): Response
                  )
                }
              }
          } ~
            parameters(
              Symbol("error"),
              Symbol("error_description") ?,
              Symbol("error_uri").as[Uri] ?,
              Symbol("state") ?,
            )
              .as[Response.Error](Response.Error) { resp =>
                complete(ErrorResponse(resp.error): Response)
              }
        }
      },
      path("refresh") {
        post {
          entity(as[RefreshParams]) { params =>
            val body = Request.Refresh(
              grantType = "refresh_token",
              refreshToken = params.refreshToken,
              clientId = config.clientId,
              clientSecret = config.clientSecret,
            )
            val f =
              for {
                entity <- Marshal(body).to[RequestEntity]
                req = HttpRequest(
                  uri = config.authServerUrl.withPath(Path./("token")),
                  entity = entity,
                  method = HttpMethods.POST,
                )
                resp <- Http().singleRequest(req)
                tokenResp <-
                  if (resp.status != StatusCodes.OK) {
                    Unmarshal(resp).to[String].flatMap { msg =>
                      throw new RuntimeException(
                        s"Failed to fetch refresh token (${resp.status}): $msg."
                      )
                    }
                  } else {
                    Unmarshal(resp).to[Response.Token]
                  }
              } yield tokenResp
            onSuccess(f) { tokenResp =>
              // Now we have the access_token and potentially the refresh token. At this point,
              // we would start the trigger.
              complete(
                AccessResponse(
                  tokenResp.accessToken,
                  tokenResp.refreshToken.getOrElse(
                    sys.error("/token endpoint failed to return a refresh token")
                  ),
                ): Response
              )
            }
          }
        }
      },
    )
    Http().newServerAt("localhost", config.port.value).bind(route)
  }

}
