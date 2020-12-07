// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import org.scalatest.wordspec.AsyncWordSpec
import spray.json._

import scala.concurrent.Future

class Test extends AsyncWordSpec with TestFixture with SuiteResourceManagementAroundAll {
  import Client.JsonProtocol._
  private def requestToken(
      parties: Seq[String],
      admin: Boolean,
      applicationId: Option[String]): Future[Either[String, (AuthServiceJWTPayload, String)]] = {
    lazy val clientUri = Uri()
      .withAuthority(clientBinding.localAddress.getHostString, clientBinding.localAddress.getPort)
    val req = HttpRequest(
      uri = clientUri.withPath(Path./("access")).withScheme("http"),
      method = HttpMethods.POST,
      entity = HttpEntity(
        MediaTypes.`application/json`,
        Client.AccessParams(parties, admin, applicationId).toJson.compactPrint)
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
                Future.successful(_))
            payload <- Future.fromTry(AuthServiceJWTCodec.readFromString(decodedJwt.payload))
          } yield Right((payload, refreshToken))
        case Client.ErrorResponse(error) => Future(Left(error))
      }
    } yield result
  }

  private def requestRefresh(
      refreshToken: String): Future[Either[String, (AuthServiceJWTPayload, String)]] = {
    lazy val clientUri = Uri()
      .withAuthority(clientBinding.localAddress.getHostString, clientBinding.localAddress.getPort)
    val req = HttpRequest(
      uri = clientUri.withPath(Path./("refresh")).withScheme("http"),
      method = HttpMethods.POST,
      entity = HttpEntity(
        MediaTypes.`application/json`,
        Client.RefreshParams(refreshToken).toJson.compactPrint)
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
                Future.successful(_))
            payload <- Future.fromTry(AuthServiceJWTCodec.readFromString(decodedJwt.payload))
          } yield Right((payload, refreshToken))
        case Client.ErrorResponse(error) => Future(Left(error))
      }
    } yield result
  }

  private def expectToken(
      parties: Seq[String],
      admin: Boolean = false,
      applicationId: Option[String] = None): Future[(AuthServiceJWTPayload, String)] =
    requestToken(parties, admin, applicationId).flatMap {
      case Left(error) => fail(s"Expected token but got error-code $error")
      case Right(token) => Future(token)
    }

  private def expectError(
      parties: Seq[String],
      admin: Boolean = false,
      applicationId: Option[String] = None): Future[String] =
    requestToken(parties, admin, applicationId).flatMap {
      case Left(error) => Future(error)
      case Right(_) => fail("Expected an error but got a token")
    }

  private def expectRefresh(refreshToken: String): Future[(AuthServiceJWTPayload, String)] =
    requestRefresh(refreshToken).flatMap {
      case Left(error) => fail(s"Expected token but got error-code $error")
      case Right(token) => Future(token)
    }

  "the auth server" should {
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
    "refresh a token" in {
      for {
        (token1, refresh1) <- expectToken(Seq())
        _ <- Future(clock.set(token1.exp.get.plusSeconds(1)))
        (token2, _) <- expectRefresh(refresh1)
      } yield {
        assert(token2.exp.get.isAfter(token1.exp.get))
        assert(token1.copy(exp = None) == token2.copy(exp = None))
      }
    }
    "return a token with the requested app id" in {
      for {
        (token, __) <- expectToken(Seq(), applicationId = Some("my-app-id"))
      } yield {
        assert(token.applicationId == Some("my-app-id"))
      }
    }
    "return a token with no app id if non is requested" in {
      for {
        (token, __) <- expectToken(Seq(), applicationId = None)
      } yield {
        assert(token.applicationId == None)
      }
    }
  }
}
