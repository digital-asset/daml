// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.server

import java.util.UUID

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshaller
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.refinements.ApiTypes.Party

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

// This is a test authorization server that implements the OAuth2 authorization code flow.
// This is primarily intended for use in the trigger service tests but could also serve
// as a useful ground for experimentation.
// Given scopes of the form `actAs:$party`, the authorization server will issue
// tokens with the respective claims. Requests for authorized parties will be accepted and
// request to /authorize are immediately redirected to the redirect_uri.
class Server(config: Config) {
  private val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""

  // To keep things as simple as possible, we use a UUID as the authorization code
  // and in the /authorize request we already pre-compute the JWT payload based on the scope.
  // The token request then only does a lookup and signs the token.
  private var requests = Map.empty[UUID, AuthServiceJWTPayload]

  private def requestParties(req: Request.Authorize): List[Party] =
    Party.subst {
      req.scope.fold(List.empty[String])(s => List[String](s.split(" "): _*)).collect {
        case s if s.startsWith("actAs:") => s.stripPrefix("actAs:")
      }
    }

  private def toPayload(req: Request.Authorize): AuthServiceJWTPayload = {
    val parties: List[String] =
      req.scope.fold(List.empty[String])(s => List[String](s.split(" "): _*)).collect {
        case s if s.startsWith("actAs:") => s.stripPrefix("actAs:")
      }
    AuthServiceJWTPayload(
      ledgerId = Some(config.ledgerId),
      applicationId = config.applicationId,
      // Not required by the default auth service
      participantId = None,
      // Only for testing, expire never.
      exp = None,
      // no admin claim for now.
      admin = false,
      actAs = parties,
      readAs = List()
    )
  }

  import Request.Token.unmarshalHttpEntity
  implicit val unmarshal: Unmarshaller[String, Uri] = Unmarshaller.strict(Uri(_))

  val route = concat(
    path("authorize") {
      get {
        parameters(
          (
            'response_type,
            'client_id,
            'redirect_uri.as[Uri],
            'scope ?,
            'state ?,
            'audience.as[Uri] ?))
          .as[Request.Authorize](Request.Authorize) {
            request =>
              val parties = requestParties(request)
              val denied = config.parties.map(parties.toSet -- _).getOrElse(Nil)
              if (denied.isEmpty) {
                val authorizationCode = UUID.randomUUID()
                val params =
                  Response
                    .Authorize(code = authorizationCode.toString, state = request.state)
                    .toQuery
                requests += (authorizationCode -> toPayload(request))
                // We skip any actual consent screen since this is only intended for testing and
                // this is outside of the scope of the trigger service anyway.
                redirect(request.redirectUri.withQuery(params), StatusCodes.Found)
              } else {
                val params =
                  Response
                    .Error(
                      error = "access_denied",
                      errorDescription = Some(s"Access to parties ${denied.mkString(" ")} denied"),
                      errorUri = None,
                      state = request.state)
                    .toQuery
                redirect(request.redirectUri.withQuery(params), StatusCodes.Found)
              }
          }
      }
    },
    path("token") {
      post {
        entity(as[Request.Token]) {
          request =>
            // No validation to keep things simple
            requests.get(UUID.fromString(request.code)) match {
              case None => sys.exit(2)
              case Some(payload) =>
                import JsonProtocol._
                complete(
                  Response.Token(
                    accessToken = JwtSigner.HMAC256
                      .sign(
                        DecodedJwt(jwtHeader, AuthServiceJWTCodec.compactPrint(payload)),
                        config.jwtSecret)
                      .getOrElse(
                        throw new IllegalArgumentException("Failed to sign a token")
                      )
                      .value,
                    refreshToken = None,
                    expiresIn = None,
                    scope = None,
                    tokenType = "bearer"
                  ))
            }
        }
      }
    }
  )

  def start()(implicit system: ActorSystem): Future[ServerBinding] = {
    Http().bindAndHandle(route, "localhost", config.port.value)
  }
}

object Server {
  def apply(config: Config) = new Server(config)
  def stop(f: Future[ServerBinding])(implicit ec: ExecutionContext): Future[Done] =
    f.flatMap(_.unbind())
}
