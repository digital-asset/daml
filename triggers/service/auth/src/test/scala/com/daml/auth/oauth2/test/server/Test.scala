// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.auth.oauth2.test.server

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.daml.jwt.JwtDecoder
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.{
  AuthServiceJWTCodec,
  CustomDamlJWTPayload,
  AuthServiceJWTPayload,
  StandardJWTPayload,
}
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import org.scalatest.wordspec.AsyncWordSpec
import spray.json._

import java.time.Instant
import scala.concurrent.Future
import scala.util.Try

abstract class Test extends AsyncWordSpec with TestFixture with SuiteResourceManagementAroundAll {
  import Client.JsonProtocol._
  import Test._

  type Tok <: AuthServiceJWTPayload
  protected[this] val Tok: TokenCompat[Tok]
  implicit def `default Token`: Token[Tok]

  private def readJWTTokenFromString[A](
      serializedPayload: String
  )(implicit A: Token[A]): Try[A] =
    AuthServiceJWTCodec.readFromString(serializedPayload).flatMap { t => Try(A.run(t)) }

  private def requestToken[A: Token](
      parties: Seq[String],
      admin: Boolean,
      applicationId: Option[String],
  ): Future[Either[String, (A, String)]] = {
    lazy val clientUri = Uri()
      .withAuthority(clientBinding.localAddress.getHostString, clientBinding.localAddress.getPort)
    val req = HttpRequest(
      uri = clientUri.withPath(Path./("access")).withScheme("http"),
      method = HttpMethods.POST,
      entity = HttpEntity(
        MediaTypes.`application/json`,
        Client.AccessParams(parties, admin, applicationId).toJson.compactPrint,
      ),
    )
    for {
      resp <- Http().singleRequest(req)
      // Redirect to /authorize on authorization server (No automatic redirect handling in akka-http)
      resp <- {
        assert(resp.status == StatusCodes.Found)
        val req = HttpRequest(uri = resp.header[Location].get.uri)
        Http().singleRequest(req)
      }
      // Redirect to /cb on client.
      resp <- {
        assert(resp.status == StatusCodes.Found)
        val req = HttpRequest(uri = resp.header[Location].get.uri)
        Http().singleRequest(req)
      }
      // Actual token response (proxied from auth server to us via the client)
      body <- Unmarshal(resp).to[Client.Response]
      result <- body match {
        case Client.AccessResponse(token, refreshToken) =>
          for {
            decodedJwt <- JwtDecoder
              .decode(Jwt(token))
              .fold(
                e => Future.failed(new IllegalArgumentException(e.toString)),
                Future.successful(_),
              )
            payload <- Future.fromTry(readJWTTokenFromString[A](decodedJwt.payload))
          } yield Right((payload, refreshToken))
        case Client.ErrorResponse(error) => Future(Left(error))
      }
    } yield result
  }

  private def requestRefresh[A: Token](
      refreshToken: String
  ): Future[Either[String, (A, String)]] = {
    lazy val clientUri = Uri()
      .withAuthority(clientBinding.localAddress.getHostString, clientBinding.localAddress.getPort)
    val req = HttpRequest(
      uri = clientUri.withPath(Path./("refresh")).withScheme("http"),
      method = HttpMethods.POST,
      entity = HttpEntity(
        MediaTypes.`application/json`,
        Client.RefreshParams(refreshToken).toJson.compactPrint,
      ),
    )
    for {
      resp <- Http().singleRequest(req)
      // Token response (proxied from auth server to us via the client)
      body <- Unmarshal(resp).to[Client.Response]
      result <- body match {
        case Client.AccessResponse(token, refreshToken) =>
          for {
            decodedJwt <- JwtDecoder
              .decode(Jwt(token))
              .fold(
                e => Future.failed(new IllegalArgumentException(e.toString)),
                Future.successful(_),
              )
            payload <- Future.fromTry(readJWTTokenFromString[A](decodedJwt.payload))
          } yield Right((payload, refreshToken))
        case Client.ErrorResponse(error) => Future(Left(error))
      }
    } yield result
  }

  protected[this] def expectToken(
      parties: Seq[String],
      admin: Boolean = false,
      applicationId: Option[String] = None,
  ): Future[(Tok, String)] =
    requestToken(parties, admin, applicationId).flatMap {
      case Left(error) => fail(s"Expected token but got error-code $error")
      case Right(token) => Future(token)
    }

  protected[this] def expectError(
      parties: Seq[String],
      admin: Boolean = false,
      applicationId: Option[String] = None,
  ): Future[String] =
    requestToken[AuthServiceJWTPayload](parties, admin, applicationId).flatMap {
      case Left(error) => Future(error)
      case Right(_) => fail("Expected an error but got a token")
    }

  private def expectRefresh(refreshToken: String): Future[(Tok, String)] =
    requestRefresh(refreshToken).flatMap {
      case Left(error) => fail(s"Expected token but got error-code $error")
      case Right(token) => Future(token)
    }

  "the auth server" should {
    "refresh a token" in {
      for {
        (token1, refresh1) <- expectToken(Seq())
        _ <- Future(clock.set((Tok exp token1) plusSeconds 1))
        (token2, _) <- expectRefresh(refresh1)
      } yield {
        assert((Tok exp token2) isAfter (Tok exp token1))
        assert((Tok withoutExp token1) == (Tok withoutExp token2))
      }
    }
    "return a token with the requested app id" in {
      for {
        (token, __) <- expectToken(Seq(), applicationId = Some("my-app-id"))
      } yield {
        assert(Tok.userId(token) == Some("my-app-id"))
      }
    }
    "return a token with no app id if non is requested" in {
      for {
        (token, __) <- expectToken(Seq(), applicationId = None)
      } yield {
        assert(Tok.userId(token) == None)
      }
    }

  }
}

class ClaimTokenTest extends Test {
  import Test._

  override def yieldUserTokens = false

  type Tok = CustomDamlJWTPayload
  override object Tok extends TokenCompat[Tok] {
    override def userId(t: Tok) = t.applicationId
    override def exp(t: Tok) = t.exp.get
    override def withoutExp(t: Tok) = t copy (exp = None)
  }

  implicit override def `default Token`: Token[Tok] = new Token({
    case _: StandardJWTPayload =>
      throw new IllegalStateException(
        "auth-middleware: user access tokens are not expected here"
      )
    case payload: CustomDamlJWTPayload => payload
  })

  "the auth server with claim tokens" should {
    "issue a token with no parties" in {
      for {
        (token, _) <- expectToken(Seq())
      } yield {
        assert(token.actAs == Seq())
      }
    }
    "issue a token with 1 party" in {
      for {
        (token, _) <- expectToken(Seq("Alice"))
      } yield {
        assert(token.actAs == Seq("Alice"))
      }
    }
    "issue a token with multiple parties" in {
      for {
        (token, _) <- expectToken(Seq("Alice", "Bob"))
      } yield {
        assert(token.actAs == Seq("Alice", "Bob"))
      }
    }
    "deny access to unauthorized parties" in {
      server.revokeParty(Party("Eve"))
      for {
        error <- expectError(Seq("Alice", "Eve"))
      } yield {
        assert(error == "access_denied")
      }
    }
    "issue a token with admin access" in {
      for {
        (token, _) <- expectToken(Seq(), admin = true)
      } yield {
        assert(token.admin)
      }
    }
    "deny admin access if unauthorized" in {
      server.revokeAdmin()
      for {
        error <- expectError(Seq(), admin = true)
      } yield {
        assert(error == "access_denied")
      }
    }
  }
}

class UserTokenTest extends Test {
  import Test._

  override def yieldUserTokens = true

  type Tok = StandardJWTPayload
  override object Tok extends TokenCompat[Tok] {
    override def userId(t: Tok) = Some(t.userId).filter(_.nonEmpty)
    override def exp(t: Tok) = t.exp.get
    override def withoutExp(t: Tok) = t copy (exp = None)
  }

  implicit override def `default Token`: Token[Tok] = new Token({
    case payload: StandardJWTPayload => payload
    case _: CustomDamlJWTPayload =>
      throw new IllegalStateException(
        "auth-middleware: custom tokens are not expected here"
      )
  })

  "the auth server with user tokens" should {}
}

object Test {
  final class Token[A](val run: AuthServiceJWTPayload => A) extends AnyVal

  object Token {
    implicit val any: Token[AuthServiceJWTPayload] = new Token(identity)
  }

  private[server] abstract class TokenCompat[Tok] {
    def userId(t: Tok): Option[String]
    def exp(t: Tok): Instant
    def withoutExp(t: Tok): Tok
  }
}
