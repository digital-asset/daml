// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.daml.http.json.{JsonError, SprayJson}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.timer.RetryStrategy
import org.scalatest._
import org.scalatest.concurrent.Eventually
import scalaz.\/
import scalaz.syntax.show._
import scalaz.syntax.tag._
import spray.json._

import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
final class FailureTests
    extends AsyncFreeSpec
    with HttpFailureTestFixture
    with Matchers
    with SuiteResourceManagementAroundAll
    with Eventually
    with Inside {
  import HttpServiceTestFixture._

  "Command submission succeeds after reconnect" in withHttpService[Assertion] {
    (uri, encoder, _, client) =>
      for {
        p <- allocateParty(client, "Alice")
        (status, _) <- postCreateCommand(
          accountCreateCommand(p, "23"),
          encoder,
          uri,
          headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
        _ = status shouldBe StatusCodes.OK
        _ = proxy.disable()
        (status, output) <- postCreateCommand(
          accountCreateCommand(p, "24"),
          encoder,
          uri,
          headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
        _ = status shouldBe StatusCodes.InternalServerError
        _ <- inside(output) {
          case JsObject(fields) =>
            inside(fields.get("status")) {
              case Some(JsNumber(code)) => code shouldBe 500
            }
        }
        _ = proxy.enable()
        // eventually doesn’t handle Futures in the version of scalatest we’re using.
        _ <- RetryStrategy.constant(5, 2.seconds)((_, _) =>
          for {
            (status, _) <- postCreateCommand(
              accountCreateCommand(p, "25"),
              encoder,
              uri,
              headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
          } yield status shouldBe StatusCodes.OK)
      } yield succeed
  }

  "/v1/query GET succeeds after reconnect" in withHttpService[Assertion] {
    (uri, encoder, _, client) =>
      for {
        p <- allocateParty(client, "Alice")
        (status, _) <- postCreateCommand(
          accountCreateCommand(p, "23"),
          encoder,
          uri,
          headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
        (status, output) <- getRequest(
          uri = uri.withPath(Uri.Path("/v1/query")),
          headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
        _ <- inside(output) {
          case JsObject(fields) =>
            inside(fields.get("result")) {
              case Some(JsArray(rs)) => rs.size shouldBe 1
            }
        }
        _ = proxy.disable()
        (status, output) <- getRequest(
          uri = uri.withPath(Uri.Path("/v1/query")),
          headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
        _ <- inside(output) {
          case JsObject(fields) =>
            inside(fields.get("status")) {
              case Some(JsNumber(code)) => code shouldBe 501
            }
        }
        // TODO Document this properly or adjust it
        _ = status shouldBe StatusCodes.OK
        _ = proxy.enable()
      } yield succeed
  }

  "/v1/query POST succeeds after reconnect" in withHttpService[Assertion] {
    (uri, encoder, _, client) =>
      for {
        p <- allocateParty(client, "Alice")
        (status, _) <- postCreateCommand(
          accountCreateCommand(p, "23"),
          encoder,
          uri,
          headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
        _ = status shouldBe StatusCodes.OK
        query = jsObject("""{"templateIds": ["Account:Account"]}""")
        _ = println("first query")
        (status, output) <- postRequest(
          uri = uri.withPath(Uri.Path("/v1/query")),
          query,
          headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
        _ = status shouldBe StatusCodes.OK
        _ <- inside(output) {
          case JsObject(fields) =>
            inside(fields.get("result")) {
              case Some(JsArray(rs)) => rs.size shouldBe 1
            }
        }
        _ = proxy.disable()
        (status, output) <- postRequest(
          uri = uri.withPath(Uri.Path("/v1/query")),
          query,
          headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
        _ <- inside(output) {
          case JsObject(fields) =>
            inside(fields.get("status")) {
              case Some(JsNumber(code)) => code shouldBe 501
            }
        }
        // TODO Document this properly or adjust it
        _ = status shouldBe StatusCodes.OK
        _ = proxy.enable()
        // eventually doesn’t handle Futures in the version of scalatest we’re using.
        _ <- RetryStrategy.constant(5, 2.seconds)((_, _) =>
          for {
            (status, output) <- postRequest(
              uri = uri.withPath(Uri.Path("/v1/query")),
              query,
              headersWithPartyAuth(List(p.unwrap), ledgerId().unwrap))
            _ = status shouldBe StatusCodes.OK
            _ <- inside(output) {
              case JsObject(fields) =>
                inside(fields.get("result")) {
                  case Some(JsArray(rs)) => rs.size shouldBe 1
                }
            }
          } yield succeed)
      } yield succeed
  }

  protected def jsObject(s: String): JsObject = {
    val r: JsonError \/ JsObject = for {
      jsVal <- SprayJson.parse(s).leftMap(e => JsonError(e.shows))
      jsObj <- SprayJson.mustBeJsObject(jsVal)
    } yield jsObj
    r.valueOr(e => fail(e.shows))
  }
}
