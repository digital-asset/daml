// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.middleware

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpCookie, HttpCookiePair}
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import com.daml.oauth.server.{Request => OAuthRequest, Response => OAuthResponse}
import com.typesafe.scalalogging.StrictLogging
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Try

// This is an implementation of the trigger service authentication middleware
// for OAuth2 as specified in `/triggers/service/authentication.md`
object Server extends StrictLogging {
  import JsonProtocol._
  import com.daml.oauth.server.JsonProtocol._
  implicit private val unmarshal: Unmarshaller[String, Uri] = Unmarshaller.strict(Uri(_))

  // TODO[AH] Make the redirect URI configurable, especially the authority. E.g. when running behind nginx.
  private def toRedirectUri(uri: Uri) =
    Uri()
      .withScheme(uri.scheme)
      .withAuthority(uri.authority)
      .withPath(Uri.Path./("cb"))

  def start(
      config: Config)(implicit system: ActorSystem, ec: ExecutionContext): Future[ServerBinding] = {
    // TODO[AH] Make sure this is bounded in size - or avoid state altogether.
    val requests: TrieMap[UUID, Uri] = TrieMap()
    val route = concat(
      path("auth") { get { auth } },
      path("login") { get { login(config, requests) } },
      path("cb") { get { loginCallback(config, requests) } },
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

  private val cookieName = "daml-ledger-token"

  private def optionalToken: Directive1[Option[OAuthResponse.Token]] = {
    def f(x: HttpCookiePair) = OAuthResponse.Token.fromCookieValue(x.value)
    optionalCookie(cookieName).map(_.flatMap(f))
  }

  private def auth =
    parameters(('claims))
      .as[Request.Auth](Request.Auth) { auth =>
        optionalToken {
          // TODO[AH] Implement mapping from scope to claims
          // TODO[AH] Check whether granted scope subsumes requested claims
          case Some(token) if token.scope == Some(auth.claims) =>
            complete(
              Response
                .Authorize(accessToken = token.accessToken, refreshToken = token.refreshToken))
          case _ => complete(StatusCodes.Unauthorized)
        }
      }

  private def login(config: Config, requests: TrieMap[UUID, Uri]) =
    parameters(('redirect_uri.as[Uri], 'claims))
      .as[Request.Login](Request.Login) { login =>
        extractRequest { request =>
          val requestId = UUID.randomUUID
          requests += (requestId -> login.redirectUri)
          val authorize = OAuthRequest.Authorize(
            responseType = "code",
            clientId = config.clientId,
            redirectUri = toRedirectUri(request.uri),
            scope = Some(login.claims),
            state = Some(requestId.toString))
          redirect(config.oauthAuth.withQuery(authorize.toQuery), StatusCodes.Found)
        }
      }

  private def loginCallback(config: Config, requests: TrieMap[UUID, Uri])(
      implicit system: ActorSystem,
      ec: ExecutionContext) =
    // TODO[AH] Implement error response handler https://tools.ietf.org/html/rfc6749#section-4.1.2.1
    parameters(('code, 'state ?))
      .as[OAuthResponse.Authorize](OAuthResponse.Authorize) { authorize =>
        extractRequest { request =>
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
              import com.daml.oauth.server.Request.Token.marshalRequestEntity
              val tokenRequest = for {
                entity <- Marshal(body).to[RequestEntity]
                req = HttpRequest(
                  uri = config.oauthToken,
                  entity = entity,
                  method = HttpMethods.POST)
                resp <- Http().singleRequest(req)
                tokenResp <- if (resp.status != StatusCodes.OK) {
                  Unmarshal(resp).to[String].flatMap { msg =>
                    Future.failed(new RuntimeException(
                      s"Failed to retrieve token at ${req.uri} (${resp.status}): $msg"))
                  }
                } else {
                  Unmarshal(resp).to[OAuthResponse.Token]
                }
              } yield tokenResp
              onSuccess(tokenRequest) { token =>
                setCookie(HttpCookie(cookieName, token.toCookieValue)) {
                  redirect(redirectUri, StatusCodes.Found)
                }
              }
          }
        }
      }
}
