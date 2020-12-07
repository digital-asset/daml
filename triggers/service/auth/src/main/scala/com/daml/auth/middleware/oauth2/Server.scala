// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.oauth2

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
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.auth.oauth2.api.{Request => OAuthRequest, Response => OAuthResponse}
import com.typesafe.scalalogging.StrictLogging
import java.util.UUID

import com.daml.auth.middleware.api.{Request, Response}
import com.daml.jwt.{JwtDecoder, JwtVerifierBase}
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.AuthServiceJWTCodec

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Try

// This is an implementation of the trigger service authentication middleware
// for OAuth2 as specified in `/triggers/service/authentication.md`
object Server extends StrictLogging {
  import com.daml.auth.middleware.api.JsonProtocol._
  import com.daml.auth.oauth2.api.JsonProtocol._
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
      path("auth") {
        get {
          auth(config)
        }
      },
      path("login") {
        get {
          login(config, requests)
        }
      },
      path("cb") {
        get {
          loginCallback(config, requests)
        }
      },
      path("refresh") {
        post {
          refresh(config)
        }
      },
    )

    Http().newServerAt("localhost", config.port.value).bind(route)
  }

  def stop(f: Future[ServerBinding])(implicit ec: ExecutionContext): Future[Done] =
    f.flatMap(_.unbind())

  private val cookieName = "daml-ledger-token"

  private def optionalToken: Directive1[Option[OAuthResponse.Token]] = {
    def f(x: HttpCookiePair) = OAuthResponse.Token.fromCookieValue(x.value)
    optionalCookie(cookieName).map(_.flatMap(f))
  }

  // Check whether the provided token's signature is valid.
  private def tokenIsValid(accessToken: String, verifier: JwtVerifierBase): Boolean = {
    verifier.verify(Jwt(accessToken)).isRight
  }

  // Check whether the provided access token grants at least the requested claims.
  private def tokenProvidesClaims(accessToken: String, claims: Request.Claims): Boolean = {
    for {
      decodedJwt <- JwtDecoder.decode(Jwt(accessToken)).toOption
      tokenPayload <- AuthServiceJWTCodec.readFromString(decodedJwt.payload).toOption
    } yield {
      (tokenPayload.admin || !claims.admin) &&
      tokenPayload.actAs.toSet.subsetOf(claims.actAs.map(_.toString).toSet) &&
      tokenPayload.readAs.toSet.subsetOf(claims.readAs.map(_.toString).toSet) &&
      ((claims.applicationId, tokenPayload.applicationId) match {
        // No requirement on app id
        case (None, _) => true
        // Token valid for all app ids.
        case (_, None) => true
        case (Some(expectedAppId), Some(actualAppId)) => expectedAppId == ApplicationId(actualAppId)
      })
    }
  }.getOrElse(false)

  private def auth(config: Config) =
    parameters('claims.as[Request.Claims])
      .as[Request.Auth](Request.Auth) { auth =>
        optionalToken {
          case Some(token)
              if tokenIsValid(token.accessToken, config.tokenVerifier) &&
                tokenProvidesClaims(token.accessToken, auth.claims) =>
            complete(
              Response
                .Authorize(accessToken = token.accessToken, refreshToken = token.refreshToken))
          // TODO[AH] Include a `WWW-Authenticate` header.
          case _ => complete(StatusCodes.Unauthorized)
        }
      }

  private def login(config: Config, requests: TrieMap[UUID, Uri]) =
    parameters('redirect_uri.as[Uri], 'claims.as[Request.Claims], 'state ?)
      .as[Request.Login](Request.Login) { login =>
        extractRequest { request =>
          val requestId = UUID.randomUUID
          requests += (requestId -> {
            var query = login.redirectUri.query().to[Seq]
            login.state.foreach(x => query ++= Seq("state" -> x))
            login.redirectUri.withQuery(Uri.Query(query: _*))
          })
          val authorize = OAuthRequest.Authorize(
            responseType = "code",
            clientId = config.clientId,
            redirectUri = toRedirectUri(request.uri),
            // Auth0 will only issue a refresh token if the offline_access claim is present.
            // TODO[AH] Make the request configurable, see https://github.com/digital-asset/daml/issues/7957
            scope = Some("offline_access " + login.claims.toQueryString),
            state = Some(requestId.toString),
            audience = Some("https://daml.com/ledger-api")
          )
          redirect(config.oauthAuth.withQuery(authorize.toQuery), StatusCodes.Found)
        }
      }

  private def loginCallback(config: Config, requests: TrieMap[UUID, Uri])(
      implicit system: ActorSystem,
      ec: ExecutionContext) = {
    def popRequest(optState: Option[String]): Directive1[Uri] = {
      val redirectUri = for {
        state <- optState
        requestId <- Try(UUID.fromString(state)).toOption
        redirectUri <- requests.remove(requestId)
      } yield redirectUri
      redirectUri match {
        case Some(redirectUri) => provide(redirectUri)
        case None => complete(StatusCodes.NotFound)
      }
    }

    concat(
      parameters('code, 'state ?)
        .as[OAuthResponse.Authorize](OAuthResponse.Authorize) { authorize =>
          popRequest(authorize.state) {
            redirectUri =>
              extractRequest {
                request =>
                  val body = OAuthRequest.Token(
                    grantType = "authorization_code",
                    code = authorize.code,
                    redirectUri = toRedirectUri(request.uri),
                    clientId = config.clientId,
                    clientSecret = config.clientSecret)
                  import com.daml.auth.oauth2.api.Request.Token.marshalRequestEntity
                  val tokenRequest =
                    for {
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
        },
      parameters('error, 'error_description ?, 'error_uri.as[Uri] ?, 'state ?)
        .as[OAuthResponse.Error](OAuthResponse.Error) { error =>
          popRequest(error.state) { redirectUri =>
            val uri = redirectUri.withQuery {
              var params = redirectUri.query().to[Seq]
              params ++= Seq("error" -> error.error)
              error.errorDescription.foreach(x => params ++= Seq("error_description" -> x))
              Uri.Query(params: _*)
            }
            redirect(uri, StatusCodes.Found)
          }
        }
    )
  }

  private def refresh(config: Config)(implicit system: ActorSystem, ec: ExecutionContext) =
    entity(as[Request.Refresh]) { refresh =>
      val body = OAuthRequest.Refresh(
        grantType = "refresh_token",
        refreshToken = refresh.refreshToken,
        clientId = config.clientId,
        clientSecret = config.clientSecret)
      import com.daml.auth.oauth2.api.Request.Refresh.marshalRequestEntity
      val tokenRequest =
        for {
          entity <- Marshal(body).to[RequestEntity]
          req = HttpRequest(uri = config.oauthToken, entity = entity, method = HttpMethods.POST)
          resp <- Http().singleRequest(req)
        } yield resp
      onSuccess(tokenRequest) { resp =>
        resp.status match {
          // Return access and refresh token on success.
          case StatusCodes.OK =>
            val authResponse = Unmarshal(resp).to[OAuthResponse.Token].map { token =>
              Response.Authorize(accessToken = token.accessToken, refreshToken = token.refreshToken)
            }
            complete(authResponse)
          // Forward client errors.
          case status: StatusCodes.ClientError =>
            complete(HttpResponse.apply(status = status, entity = resp.entity))
          // Fail on unexpected responses.
          case _ =>
            onSuccess(Unmarshal(resp).to[String]) { msg =>
              failWith(
                new RuntimeException(s"Failed to retrieve refresh token (${resp.status}): $msg"))
            }
        }
      }
    }
}
