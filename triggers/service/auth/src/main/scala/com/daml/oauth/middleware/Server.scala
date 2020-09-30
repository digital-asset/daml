// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.HttpCookie
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import com.daml.oauth.server.{Request => OAuthRequest, Response => OAuthResponse}
import com.typesafe.scalalogging.StrictLogging
import java.util.Base64
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Try
import spray.json._

// This is an implementation of the trigger service authentication middleware
// for OAuth2 as specified in `/triggers/service/authentication.md`
object Server extends StrictLogging {
  import com.daml.oauth.server.JsonProtocol._

  // TODO[AH] Make the redirect URI configurable, especially the authority. E.g. when running behind nginx.
  private def toRedirectUri(uri: Uri) =
    Uri()
      .withScheme(uri.scheme)
      .withAuthority(uri.authority)
      .withPath(Uri.Path./("cb"))

  def start(
      config: Config)(implicit system: ActorSystem, ec: ExecutionContext): Future[ServerBinding] = {
    implicit val unmarshal: Unmarshaller[String, Uri] = Unmarshaller.strict(Uri(_))
    // TODO[AH] Make sure this is bounded in size - or avoid state altogether.
    val requests = TrieMap[UUID, Uri]()
    val route = concat(
      path("auth") {
        get {
          complete((StatusCodes.NotImplemented, "The /auth endpoint is not implemented yet"))
        }
      },
      path("login") {
        get {
          parameters(('redirect_uri.as[Uri], 'claims))
            .as[Request.Login](Request.Login) {
              login =>
                extractRequest {
                  request =>
                    val requestId = UUID.randomUUID
                    requests += (requestId -> login.redirectUri)
                    val authorize = OAuthRequest.Authorize(
                      responseType = "code",
                      clientId = config.clientId,
                      redirectUri = toRedirectUri(request.uri),
                      scope = Some(login.claims),
                      state = Some(requestId.toString))
                    redirect(
                      config.oauthUri
                        .withPath(Uri.Path./("authorize"))
                        .withQuery(authorize.toQuery),
                      StatusCodes.Found)
                }
            }
        }
      },
      path("cb") {
        get {
          parameters(('code, 'state ?))
            .as[OAuthResponse.Authorize](OAuthResponse.Authorize) {
              authorize =>
                extractRequest {
                  request =>
                    val redirectUri = for {
                      state <- authorize.state
                      requestId <- Try(UUID.fromString(state)).toOption
                      redirectUri <- requests.remove(requestId)
                    } yield redirectUri
                    redirectUri match {
                      case None =>
                        complete(StatusCodes.NotFound)
                      case Some(redirectUri) =>
                        val body = OAuthRequest.Token(
                          grantType = "authorization_code",
                          code = authorize.code,
                          redirectUri = toRedirectUri(request.uri),
                          clientId = config.clientId,
                          clientSecret = config.clientSecret)
                        val req = HttpRequest(
                          uri = config.oauthUri.withPath(Uri.Path./("token")),
                          entity =
                            HttpEntity(MediaTypes.`application/json`, body.toJson.compactPrint),
                          method = HttpMethods.POST)
                        val tokenRequest = for {
                          resp <- Http().singleRequest(req)
                          tokenResp <- Unmarshal(resp).to[OAuthResponse.Token]
                        } yield tokenResp
                        onSuccess(tokenRequest) { token =>
                          val encoder = Base64.getUrlEncoder()
                          val content = encoder.encodeToString(token.toJson.compactPrint.getBytes)
                          setCookie(HttpCookie("daml-ledger-token", content)) {
                            redirect(redirectUri, StatusCodes.Found)
                          }
                        }
                    }
                }
            }
        }
      },
      path("refresh") {
        get {
          complete((StatusCodes.NotImplemented, "The /refresh endpoint is not implemented yet"))
        }
      }
    )

    Http().bindAndHandle(route, "localhost", config.port.value)
  }
  def stop(f: Future[ServerBinding])(implicit ec: ExecutionContext): Future[Done] =
    f.flatMap(_.unbind())
}
