// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.middleware.api

import java.util.UUID

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import org.apache.pekko.http.scaladsl.model.Uri.Path
import org.apache.pekko.http.scaladsl.model.{
  HttpMethods,
  HttpRequest,
  MediaTypes,
  RequestEntity,
  StatusCode,
  StatusCodes,
  Uri,
  headers,
}
import org.apache.pekko.http.scaladsl.server.{
  ContentNegotiator,
  Directive,
  Directive0,
  Directive1,
  Route,
  StandardRoute,
}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import com.daml.auth.middleware.api.Client.{AuthException, RedirectToLogin, RefreshException}
import com.daml.auth.middleware.api.Tagged.RefreshToken

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

/** Client component for interaction with the auth middleware
  *
  * A client of the auth middleware is typically itself a web-application that serves HTTP requests.
  * Note, a [[Client]] maintains state that needs to persist across such requests.
  * In particular, you should not create the [[Client]] instance within a [[Route]].
  *
  * This may pose a challenge when the client application uses dynamic port binding,
  * e.g. for testing purposes, as the login URI's redirect parameter may depend on the port.
  * To that end the login and request handler components that may depend on the port
  * are provided in a separate [[com.daml.auth.middleware.api.Client.Routes]] class
  * that does not need to maintain state across requests and can safely be constructed
  * within a [[Route]] using the [[routes]] family of methods.
  */
class Client(config: Client.Config) {
  private val callbacks: RequestStore[UUID, Response.Login => Route] = new RequestStore(
    config.maxAuthCallbacks,
    config.authCallbackTimeout,
  )

  /** Create a [[Client.Routes]] based on an absolute login redirect URI.
    */
  def routes(callbackUri: Uri): Client.Routes = {
    assert(
      callbackUri.isAbsolute,
      "The authorization middleware client callback URI must be absolute.",
    )
    RoutesImpl(callbackUri)
  }

  /** Create a [[Client.Routes]] based on a path to be appended to the requests URI scheme and authority.
    *
    * E.g. given `callbackPath = "cb"` and a request to `http://my.client/foo/bar`
    * the redirect URI would take the form `http://my.client/cb`.
    */
  def routesFromRequestAuthority(callbackPath: Uri.Path): Directive1[Client.Routes] =
    extractUri.map { reqUri =>
      RoutesImpl(
        Uri()
          .withScheme(reqUri.scheme)
          .withAuthority(reqUri.authority)
          .withPath(callbackPath)
      ): Client.Routes
    }

  /** Equivalent to [[routes]] if [[callbackUri]] is an absolute URI,
    * otherwise equivalent to [[routesFromRequestAuthority]].
    */
  def routesAuto(callbackUri: Uri): Directive1[Client.Routes] = {
    if (callbackUri.isAbsolute) {
      provide(routes(callbackUri))
    } else {
      routesFromRequestAuthority(callbackUri.path)
    }
  }

  private case class RoutesImpl(callbackUri: Uri) extends Client.Routes {
    val callbackHandler: Route =
      parameters(Symbol("state").as[UUID]) { requestId =>
        callbacks.pop(requestId) match {
          case None =>
            complete(StatusCodes.NotFound)
          case Some(callback) =>
            Response.Login.callbackParameters { callback }
        }
      }

    private val isHtmlRequest: Directive1[Boolean] = extractRequest.map { req =>
      val negotiator = ContentNegotiator(req.headers)
      val contentTypes = List(
        ContentNegotiator.Alternative(MediaTypes.`application/json`),
        ContentNegotiator.Alternative(MediaTypes.`text/html`),
      )
      val preferred = negotiator.pickContentType(contentTypes)
      preferred.map(_.mediaType) == Some(MediaTypes.`text/html`)
    }

    /** Pass control to the inner directive if we should redirect to login on auth failure, reject otherwise.
      */
    private val onRedirectToLogin: Directive0 =
      config.redirectToLogin match {
        case RedirectToLogin.No => reject
        case RedirectToLogin.Yes => pass
        case RedirectToLogin.Auto =>
          isHtmlRequest.flatMap {
            case false => reject
            case true => pass
          }
      }

    def authorize(claims: Request.Claims): Directive1[Client.AuthorizeResult] = {
      auth(claims).flatMap {
        // Authorization successful - pass token to continuation
        case Some(authorization) => provide(Client.Authorized(authorization))
        // Authorization failed - login and retry on callback request.
        case None =>
          onRedirectToLogin
            .tflatMap { _ =>
              // Ensure that the request is fully uploaded.
              val timeout = config.httpEntityUploadTimeout
              val maxBytes = config.maxHttpEntityUploadSize
              toStrictEntity(timeout, maxBytes).tflatMap { _ =>
                extractRequestContext.flatMap { ctx =>
                  Directive { (inner: Tuple1[Client.AuthorizeResult] => Route) =>
                    def continue(result: Client.AuthorizeResult): Route =
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
            .or(unauthorized(claims))
      }
    }

    /** This directive attempts to obtain an access token from the middleware's auth endpoint for the given claims.
      *
      * Forwards the current request's cookies. Completes with 500 on an unexpected response from the auth middleware.
      *
      * @return `None` if the request was denied otherwise `Some` access and optionally refresh token.
      */
    private def auth(claims: Request.Claims): Directive1[Option[Response.Authorize]] =
      extractExecutionContext.flatMap { implicit ec =>
        extractActorSystem.flatMap { implicit system =>
          extract(_.request.headers[headers.Cookie]).flatMap { cookies =>
            onSuccess(requestAuth(claims, cookies))
          }
        }
      }

    /** Return a 401 Unauthorized response.
      *
      * Includes a `WWW-Authenticate` header with a custom challenge to login at the auth middleware.
      * Lists the required claims in the `realm` and the login URI in the `login` parameter
      * and the auth URI in the `auth` parameter.
      *
      * The challenge is also included in the response body
      * as some browsers make it difficult to access the `WWW-Authenticate` header.
      */
    private def unauthorized(claims: Request.Claims): StandardRoute = {
      import com.daml.auth.middleware.api.JsonProtocol.responseAuthenticateChallengeFormat
      val challenge = Response.AuthenticateChallenge(
        claims,
        loginUri(claims, None, false),
        authUri(claims),
      )
      complete(
        status = StatusCodes.Unauthorized,
        headers = immutable.Seq(challenge.toHeader),
        challenge,
      )
    }

    def login(claims: Request.Claims, callback: Response.Login => Route): Route = {
      val requestId = UUID.randomUUID()
      if (callbacks.put(requestId, callback)) {
        redirect(loginUri(claims, Some(requestId)), StatusCodes.Found)
      } else {
        complete(StatusCodes.ServiceUnavailable)
      }
    }

    def loginUri(
        claims: Request.Claims,
        requestId: Option[UUID] = None,
        redirect: Boolean = true,
    ): Uri = {
      val redirectUri =
        if (redirect) { Some(callbackUri) }
        else { None }
      appendToUri(
        config.authMiddlewareExternalUri,
        Path./("login"),
        Request.Login(redirectUri, claims, requestId.map(_.toString)).toQuery,
      )
    }
  }

  /** Request authentication/authorization on the auth middleware's auth endpoint.
    *
    * @return `None` if the request was denied otherwise `Some` access and optionally refresh token.
    */
  def requestAuth(claims: Request.Claims, cookies: immutable.Seq[headers.Cookie])(implicit
      ec: ExecutionContext,
      system: ActorSystem,
  ): Future[Option[Response.Authorize]] =
    for {
      response <- Http().singleRequest(
        HttpRequest(
          method = HttpMethods.GET,
          uri = authUri(claims),
          headers = cookies,
        )
      )
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

  /** Request a token refresh on the auth middleware's refresh endpoint.
    */
  def requestRefresh(
      refreshToken: RefreshToken
  )(implicit ec: ExecutionContext, system: ActorSystem): Future[Response.Authorize] =
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
        )
      )
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

  private def appendToUri(uri: Uri, path: Uri.Path, query: Uri.Query = Uri.Query.Empty): Uri = {
    val newPath: Uri.Path = uri.path ++ path
    val newQueryParams: Seq[(String, String)] = uri.query().toSeq ++ query.toSeq
    val newQuery = Uri.Query(newQueryParams: _*)
    uri.withPath(newPath).withQuery(newQuery)
  }

  def authUri(claims: Request.Claims): Uri =
    appendToUri(
      config.authMiddlewareInternalUri,
      Path./("auth"),
      Request.Auth(claims).toQuery,
    )

  val refreshUri: Uri =
    appendToUri(config.authMiddlewareInternalUri, Path./("refresh"))
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

  /** Whether to automatically redirect to the login endpoint when authorization fails.
    *
    * [[RedirectToLogin.Auto]] redirects for HTML requests (`text/html`)
    * and returns 401 Unauthorized for JSON requests (`application/json`).
    */
  sealed trait RedirectToLogin
  object RedirectToLogin {
    object No extends RedirectToLogin
    object Yes extends RedirectToLogin
    object Auto extends RedirectToLogin
  }

  case class Config(
      authMiddlewareInternalUri: Uri,
      authMiddlewareExternalUri: Uri,
      redirectToLogin: RedirectToLogin,
      maxAuthCallbacks: Int,
      authCallbackTimeout: FiniteDuration,
      maxHttpEntityUploadSize: Long,
      httpEntityUploadTimeout: FiniteDuration,
  )

  trait Routes {

    /** Handler for the callback in a login flow.
      *
      * Note, a GET request on the `callbackUri` must map to this route.
      */
    def callbackHandler: Route

    /** This directive requires authorization for the given claims via the auth middleware.
      *
      * Authorization follows the steps defined in `triggers/service/authentication.md`.
      * 1. Ask for a token on the `/auth` endpoint and return it if granted.
      * 2a. Return 401 Unauthorized if denied and [[Client.Config.redirectToLogin]]
      *     indicates not to redirect to the login endpoint.
      * 2b. Redirect to the login endpoint if denied and [[Client.Config.redirectToLogin]]
      *     indicates to redirect to the login endpoint.
      *     In this case this will store the current continuation to proceed
      *     once the login flow completed and authentication succeeded.
      *     A route for the [[callbackHandler]] must be configured.
      */
    def authorize(claims: Request.Claims): Directive1[Client.AuthorizeResult]

    /** Redirect the client to login with the auth middleware.
      *
      * Will respond with 503 if the callback store is full ([[Client.Config.maxAuthCallbacks]]).
      *
      * @param callback Will be stored and executed once the login flow completed.
      */
    def login(claims: Request.Claims, callback: Response.Login => Route): Route

    def loginUri(
        claims: Request.Claims,
        requestId: Option[UUID] = None,
        redirect: Boolean = true,
    ): Uri
  }

  def apply(config: Config): Client = new Client(config)
}
