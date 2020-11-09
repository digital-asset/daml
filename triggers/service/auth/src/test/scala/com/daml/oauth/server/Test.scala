// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.oauth.server

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.unmarshalling.Unmarshal
import com.daml.jwt.JwtDecoder
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.{AuthServiceJWTCodec, AuthServiceJWTPayload}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import org.scalatest.AsyncWordSpec
import spray.json._

import scala.concurrent.Future

class Test extends AsyncWordSpec with TestFixture with SuiteResourceManagementAroundAll {
  import Client.JsonProtocol._
  private def requestToken(parties: Seq[String]): Future[Either[String, AuthServiceJWTPayload]] = {
    lazy val clientBinding = suiteResource.value._2.localAddress
    lazy val clientUri = Uri().withAuthority(clientBinding.getHostString, clientBinding.getPort)
    val req = HttpRequest(
      uri = clientUri.withPath(Path./("access")).withScheme("http"),
      method = HttpMethods.POST,
      entity =
        HttpEntity(MediaTypes.`application/json`, Client.AccessParams(parties).toJson.compactPrint)
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
        case Client.AccessResponse(token) =>
          for {
            decodedJwt <- JwtDecoder
              .decode(Jwt(token))
              .fold(
                e => Future.failed(new IllegalArgumentException(e.toString)),
                Future.successful(_))
            payload <- Future.fromTry(AuthServiceJWTCodec.readFromString(decodedJwt.payload))
          } yield Right(payload)
        case Client.ErrorResponse(error) => Future(Left(error))
      }
    } yield result
  }

  private def expectToken(parties: Seq[String]): Future[AuthServiceJWTPayload] =
    requestToken(parties).flatMap {
      case Left(error) => fail(s"Expected token but got error-code $error")
      case Right(token) => Future(token)
    }

  private def expectError(parties: Seq[String]): Future[String] =
    requestToken(parties).flatMap {
      case Left(error) => Future(error)
      case Right(_) => fail("Expected an error but got a token")
    }

  "the auth server" should {
    "issue a token with no parties" in {
      for {
        token <- expectToken(Seq())
      } yield {
        assert(token.actAs == Seq())
      }
    }
    "issue a token with 1 party" in {
      for {
        token <- expectToken(Seq("Alice"))
      } yield {
        assert(token.actAs == Seq("Alice"))
      }
    }
    "issue a token with multiple parties" in {
      for {
        token <- expectToken(Seq("Alice", "Bob"))
      } yield {
        assert(token.actAs == Seq("Alice", "Bob"))
      }
    }
    "deny access to unauthorized parties" in {
      for {
        error <- expectError(Seq("Alice", "Eve"))
      } yield {
        assert(error == "access_denied")
      }
    }
  }
}
