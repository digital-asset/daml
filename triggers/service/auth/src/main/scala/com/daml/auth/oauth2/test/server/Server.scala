// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.oauth2.test.server

import java.time.Instant
import java.util.UUID
import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.unmarshalling.Unmarshaller
import com.daml.auth.oauth2.api.{Request, Response}
import com.daml.jwt.JwtSigner
import com.daml.jwt.domain.DecodedJwt
import com.daml.ledger.api.auth.{
  AuthServiceJWTCodec,
  AuthServiceJWTPayload,
  CustomDamlJWTPayload,
  StandardJWTPayload,
  StandardJWTTokenFormat,
}
import com.daml.ledger.api.refinements.ApiTypes.Party

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

// This is a test authorization server that implements the OAuth2 authorization code flow.
// This is primarily intended for use in the trigger service tests but could also serve
// as a useful ground for experimentation.
// Given scopes of the form `actAs:$party`, the authorization server will issue
// tokens with the respective claims. Requests for authorized parties will be accepted and
// request to /authorize are immediately redirected to the redirect_uri.
class Server(config: Config) {
  import Server.withExp

  private val jwtHeader = """{"alg": "HS256", "typ": "JWT"}"""
  val tokenLifetimeSeconds = 24 * 60 * 60

  private var unauthorizedParties: Set[Party] = Set()

  // Remove the given party from the set of unauthorized parties.
  def authorizeParty(party: Party): Unit = {
    unauthorizedParties = unauthorizedParties - party
  }

  // Add the given party to the set of unauthorized parties.
  def revokeParty(party: Party): Unit = {
    unauthorizedParties = unauthorizedParties + party
  }

  // Clear the set of unauthorized parties.
  def resetAuthorizedParties(): Unit = {
    unauthorizedParties = Set()
  }

  private var allowAdmin = true

  def authorizeAdmin(): Unit = {
    allowAdmin = true
  }

  def revokeAdmin(): Unit = {
    allowAdmin = false
  }

  def resetAdmin(): Unit = {
    allowAdmin = true
  }

  // To keep things as simple as possible, we use a UUID as the authorization code and refresh token
  // and in the /authorize request we already pre-compute the JWT payload based on the scope.
  // The token request then only does a lookup and signs the token.
  private val requests = TrieMap.empty[UUID, AuthServiceJWTPayload]

  private def tokenExpiry(): Instant = {
    val now = config.clock match {
      case Some(clock) => Instant.now(clock)
      case None => Instant.now()
    }
    now.plusSeconds(tokenLifetimeSeconds.asInstanceOf[Long])
  }
  private def toPayload(req: Request.Authorize): AuthServiceJWTPayload = {
    var actAs: Seq[String] = Seq()
    var readAs: Seq[String] = Seq()
    var admin: Boolean = false
    var applicationId: Option[String] = None
    req.scope.foreach(_.split(" ").foreach {
      case s if s.startsWith("actAs:") => actAs ++= Seq(s.stripPrefix("actAs:"))
      case s if s.startsWith("readAs:") => readAs ++= Seq(s.stripPrefix("readAs:"))
      case s if s == "admin" => admin = true
      // Given that this is only for testing,
      // we don’t guard against multiple application id claims.
      case s if s.startsWith("applicationId:") =>
        applicationId = Some(s.stripPrefix("applicationId:"))
      case _ => ()
    })
    if (config.yieldUserTokens) // ignore everything but the applicationId
      StandardJWTPayload(
        issuer = None,
        userId = applicationId getOrElse "",
        participantId = None,
        exp = None,
        format = StandardJWTTokenFormat.Scope,
        audiences = List.empty,
        scope = Some("daml_ledger_api"),
      )
    else
      CustomDamlJWTPayload(
        ledgerId = Some(config.ledgerId),
        applicationId = applicationId,
        // Not required by the default auth service
        participantId = None,
        // Expiry is set when the token is retrieved
        exp = None,
        // no admin claim for now.
        admin = admin,
        actAs = actAs.toList,
        readAs = readAs.toList,
      )
  }
  // Whether the current configuration of unauthorized parties and admin rights allows to grant the given token payload.
  private def authorize(payload: AuthServiceJWTPayload): Either[String, Unit] = payload match {
    case payload: CustomDamlJWTPayload =>
      val parties = Party.subst(payload.readAs ++ payload.actAs).toSet
      val deniedParties = parties.intersect(unauthorizedParties)
      val deniedAdmin: Boolean = payload.admin && !allowAdmin
      if (deniedParties.nonEmpty) {
        Left(s"Access to parties ${deniedParties.mkString(" ")} denied")
      } else if (deniedAdmin) {
        Left("Admin access denied")
      } else {
        Right(())
      }
    case _: StandardJWTPayload => Right(())
  }

  import Request.Refresh.unmarshalHttpEntity
  implicit val unmarshal: Unmarshaller[String, Uri] = Unmarshaller.strict(Uri(_))

  val route = concat(
    path("authorize") {
      get {
        parameters(
          Symbol("response_type"),
          Symbol("client_id"),
          Symbol("redirect_uri").as[Uri],
          Symbol("scope") ?,
          Symbol("state") ?,
          Symbol("audience").as[Uri] ?,
        )
          .as[Request.Authorize](Request.Authorize) { request =>
            val payload = toPayload(request)
            authorize(payload) match {
              case Left(msg) =>
                val params =
                  Response
                    .Error(
                      error = "access_denied",
                      errorDescription = Some(msg),
                      errorUri = None,
                      state = request.state,
                    )
                    .toQuery
                redirect(request.redirectUri.withQuery(params), StatusCodes.Found)
              case Right(()) =>
                val authorizationCode = UUID.randomUUID()
                val params =
                  Response
                    .Authorize(code = authorizationCode.toString, state = request.state)
                    .toQuery
                requests.update(authorizationCode, payload)
                // We skip any actual consent screen since this is only intended for testing and
                // this is outside of the scope of the trigger service anyway.
                redirect(request.redirectUri.withQuery(params), StatusCodes.Found)
            }
          }
      }
    },
    path("token") {
      post {
        def returnToken(uuid: String) =
          Try(UUID.fromString(uuid)) match {
            case Failure(_) =>
              complete((StatusCodes.BadRequest, "Malformed code or refresh token"))
            case Success(uuid) =>
              requests.remove(uuid) match {
                case Some(payload) =>
                  // Generate refresh token
                  val refreshCode = UUID.randomUUID()
                  requests.update(refreshCode, payload)
                  // Construct access token with expiry
                  val accessToken = JwtSigner.HMAC256
                    .sign(
                      DecodedJwt(
                        jwtHeader,
                        AuthServiceJWTCodec.compactPrint(withExp(payload, Some(tokenExpiry()))),
                      ),
                      config.jwtSecret,
                    )
                    .getOrElse(throw new IllegalArgumentException("Failed to sign a token"))
                    .value
                  import com.daml.auth.oauth2.api.JsonProtocol._
                  complete(
                    Response.Token(
                      accessToken = accessToken,
                      refreshToken = Some(refreshCode.toString),
                      expiresIn = Some(tokenLifetimeSeconds),
                      scope = None,
                      tokenType = "bearer",
                    )
                  )
                case None =>
                  complete(StatusCodes.NotFound)
              }
          }
        concat(
          entity(as[Request.Token]) { request =>
            returnToken(request.code)
          },
          entity(as[Request.Refresh]) { request =>
            returnToken(request.refreshToken)
          },
        )
      }
    },
  )

  def start()(implicit system: ActorSystem): Future[ServerBinding] = {
    Http().newServerAt("localhost", config.port.value).bind(route)
  }
}

object Server {
  def apply(config: Config) = new Server(config)
  def stop(f: Future[ServerBinding])(implicit ec: ExecutionContext): Future[Done] =
    f.flatMap(_.unbind())

  private def withExp(payload: AuthServiceJWTPayload, exp: Option[Instant]): AuthServiceJWTPayload =
    payload match {
      case payload: CustomDamlJWTPayload => payload.copy(exp = exp)
      case payload: StandardJWTPayload => payload.copy(exp = exp)
    }
}
