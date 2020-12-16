// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.api

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{
  HttpMethods,
  HttpRequest,
  RequestEntity,
  StatusCode,
  StatusCodes,
  Uri,
  headers
}
import akka.http.scaladsl.server.{Directive, Directive1, Route}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.daml.auth.middleware.api.Client.{AuthException, RefreshException}
import com.daml.auth.middleware.api.Tagged.RefreshToken

import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

class Client(config: Client.Config) {
  // TODO[AH] Make sure this is bounded in size.
  private val callbacks: TrieMap[UUID, Response.Login => Route] = TrieMap()

  /**
    * Handler for the callback in a login flow.
    *
    * Note, a GET request on the `callbackUri` must map to this route.
    */
  val callbackHandler: Route =
    parameters('state.as[UUID]) { requestId =>
      callbacks.remove(requestId) match {
        case None => complete(StatusCodes.NotFound)
        case Some(callback) =>
          Response.Login.callbackParameters { callback }
      }
    }

  /**
    * This directive requires authorization for the given claims via the auth middleware.
    *
    * Authorization follows the steps defined in `triggers/service/authentication.md`.
    * First asking for a token on the `/auth` endpoint and redirecting to `/login` if none was returned.
    * If a login is required then this will store the current continuation
    * to proceed once the login flow completed and authentication succeeded.
    *
    * A route for the [[callbackHandler]] must be configured.
    */
  def authorize(claims: Request.Claims): Directive1[Client.AuthorizeResult] = {
    auth(claims).flatMap {
      // Authorization successful - pass token to continuation
      case Some(authorization) => provide(Client.Authorized(authorization))
      // Authorization failed - login and retry on callback request.
      case None =>
        // Ensure that the request is fully uploaded.
        val timeout = config.httpEntityUploadTimeout
        val maxBytes = config.maxHttpEntityUploadSize
        toStrictEntity(timeout, maxBytes).tflatMap { _ =>
          extractRequestContext.flatMap { ctx =>
            Directive { inner =>
              def continue(result: Client.AuthorizeResult) =
                mapRequestContext(_ => ctx) {
                  inner(Tuple1(result))
                }
              val callback: Response.Login => Route = {
                case Response.LoginSuccess =>
                  auth(claims) {
                    case None => continue(Client.Unauthorized)
                    case Some(authorization) => continue(Client.Authorized(authorization))
                  }
                case loginError: Response.LoginError =>
                  continue(Client.LoginFailed(loginError))
              }
              login(claims, callback)
            }
          }
        }
    }
  }

  /**
    * This directive attempts to obtain an access token from the middleware's auth endpoint for the given claims.
    *
    * Forwards the current request's cookies. Completes with 500 on an unexpected response from the auth middleware.
    *
    * @return `None` if the request was denied otherwise `Some` access and optionally refresh token.
    */
  def auth(claims: Request.Claims): Directive1[Option[Response.Authorize]] =
    extractExecutionContext.flatMap { implicit ec =>
      extractActorSystem.flatMap { implicit system =>
        extract(_.request.headers[headers.Cookie]).flatMap { cookies =>
          onSuccess(requestAuth(claims, cookies))
        }
      }
    }

  /**
    * Redirect the client to login with the auth middleware.
    *
    * @param callback Will be stored and executed once the login flow completed.
    */
  def login(claims: Request.Claims, callback: Response.Login => Route): Route = {
    val requestId = UUID.randomUUID()
    callbacks.update(requestId, callback)
    redirect(loginUri(claims, Some(requestId)), StatusCodes.Found)
  }

  /**
    * Request authentication/authorization on the auth middleware's auth endpoint.
    *
    * @return `None` if the request was denied otherwise `Some` access and optionally refresh token.
    */
  def requestAuth(claims: Request.Claims, cookies: immutable.Seq[headers.Cookie])(
      implicit ec: ExecutionContext,
      system: ActorSystem): Future[Option[Response.Authorize]] =
    for {
      response <- Http().singleRequest(
        HttpRequest(
          method = HttpMethods.GET,
          uri = authUri(claims),
          headers = cookies,
        ))
      authorize <- response.status match {
        case StatusCodes.OK =>
          import JsonProtocol.responseAuthorizeFormat
          Unmarshal(response.entity).to[Response.Authorize].map(Some(_))
        case StatusCodes.Unauthorized =>
          Future.successful(None)
        case status =>
          Unmarshal(response).to[String].flatMap { msg =>
            Future.failed(AuthException(status, msg))
          }
      }
    } yield authorize

  /**
    * Request a token refresh on the auth middleware's refresh endpoint.
    */
  def requestRefresh(refreshToken: RefreshToken)(
      implicit ec: ExecutionContext,
      system: ActorSystem): Future[Response.Authorize] =
    for {
      requestEntity <- {
        import JsonProtocol.requestRefreshFormat
        Marshal(Request.Refresh(refreshToken))
          .to[RequestEntity]
      }
      response <- Http().singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = refreshUri,
          entity = requestEntity,
        ))
      authorize <- response.status match {
        case StatusCodes.OK =>
          import JsonProtocol._
          Unmarshal(response.entity).to[Response.Authorize]
        case status =>
          Unmarshal(response).to[String].flatMap { msg =>
            Future.failed(RefreshException(status, msg))
          }
      }
    } yield authorize

  def authUri(claims: Request.Claims): Uri =
    config.authMiddlewareUri
      .withPath(Path./("auth"))
      .withQuery(Request.Auth(claims).toQuery)

  def loginUri(claims: Request.Claims, requestId: Option[UUID]): Uri =
    config.authMiddlewareUri
      .withPath(Path./("login"))
      .withQuery(Request.Login(config.callbackUri, claims, requestId.map(_.toString)).toQuery)

  val refreshUri: Uri = config.authMiddlewareUri
    .withPath(Path./("refresh"))
}

object Client {
  sealed trait AuthorizeResult
  final case class Authorized(authorization: Response.Authorize) extends AuthorizeResult
  object Unauthorized extends AuthorizeResult
  final case class LoginFailed(loginError: Response.LoginError) extends AuthorizeResult

  abstract class ClientException(message: String) extends RuntimeException(message)
  case class AuthException(status: StatusCode, message: String)
      extends ClientException(s"Failed to authorize with middleware ($status): $message")
  case class RefreshException(status: StatusCode, message: String)
      extends ClientException(s"Failed to refresh token on middleware ($status): $message")

  case class Config(
      authMiddlewareUri: Uri,
      callbackUri: Uri,
      maxHttpEntityUploadSize: Long,
      httpEntityUploadTimeout: FiniteDuration,
  )

  def apply(config: Config): Client = new Client(config)
}
