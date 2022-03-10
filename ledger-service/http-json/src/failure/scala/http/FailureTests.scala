// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.javadsl.model.ws.PeerClosedConnectionException
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink}
import com.codahale.metrics.MetricRegistry
import com.daml.dbutils.ConnectionPool

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}
import com.daml.http.domain.Offset
import com.daml.http.json.{JsonError, SprayJson}
import com.daml.http.util.FutureUtil
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.metrics.Metrics
import com.daml.timer.RetryStrategy
import eu.rekawek.toxiproxy.model.ToxicDirection
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.Eventually
import scalaz.\/
import scalaz.syntax.show._
import scalaz.syntax.tag._
import spray.json._

import scala.concurrent.duration._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
sealed abstract class FailureTests
    extends AsyncFreeSpec
    with HttpFailureTestFixture
    with HttpServiceUserFixture
    with Matchers
    with SuiteResourceManagementAroundAll
    with Eventually
    with Inside {
  import HttpServiceTestFixture.{jwtForParties => _, _}
  import WebsocketTestFixture._

  protected override final def testId = getClass.getSimpleName

  private def headersWithParties(actAs: List[String]) =
    Future successful headersWithPartyAuth(actAs, List(), Some(ledgerId().unwrap))

  "Command submission succeeds after reconnect" in withHttpService[Assertion] {
    (uri, encoder, _, client) =>
      for {
        p <- allocateParty(client, "Alice")
        (status, _) <- headersWithParties(List(p.unwrap)).flatMap(
          postCreateCommand(
            accountCreateCommand(p, "23"),
            encoder,
            uri,
            _,
          )
        )
        _ = status shouldBe StatusCodes.OK
        _ = proxy.disable()
        (status, output) <- headersWithParties(List(p.unwrap))
          .flatMap(postCreateCommand(accountCreateCommand(p, "24"), encoder, uri, _))
        _ = status shouldBe StatusCodes.ServiceUnavailable
        (status, out) <- getRequestEncoded(uri.withPath(Uri.Path("/readyz")))
        _ = status shouldBe StatusCodes.ServiceUnavailable
        _ = out shouldBe
          """[-] ledger failed (io.grpc.StatusRuntimeException: UNAVAILABLE: io exception)
            |[+] database ok
            |readyz check failed
            |""".stripMargin.replace("\r\n", "\n")
        _ <- inside(output) { case JsObject(fields) =>
          inside(fields.get("status")) { case Some(JsNumber(code)) =>
            code shouldBe 503
          }
        }
        _ = proxy.enable()
        // eventually doesn’t handle Futures in the version of scalatest we’re using.
        _ <- RetryStrategy.constant(5, 2.seconds)((_, _) =>
          for {
            (status, _) <- headersWithParties(List(p.unwrap)).flatMap(
              postCreateCommand(
                accountCreateCommand(p, "25"),
                encoder,
                uri,
                _,
              )
            )
          } yield status shouldBe StatusCodes.OK
        )
        (status, out) <- getRequestEncoded(uri.withPath(Uri.Path("/readyz")))
        _ = status shouldBe StatusCodes.OK
      } yield succeed
  }

  "Command submission timeouts" in withHttpService { (uri, encoder, _, client) =>
    import json.JsonProtocol._
    for {
      p <- allocateParty(client, "Alice")
      (status, _) <- headersWithParties(List(p.unwrap)).flatMap(
        postCreateCommand(
          accountCreateCommand(p, "23"),
          encoder,
          uri,
          _,
        )
      )
      _ = status shouldBe StatusCodes.OK
      // Client -> Server connection
      _ = proxy.toxics().timeout("timeout", ToxicDirection.UPSTREAM, 0)
      body <- FutureUtil.toFuture(
        encoder.encodeCreateCommand(accountCreateCommand(p, "24"))
      ): Future[JsValue]
      (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
        postJsonStringRequestEncoded(
          uri.withPath(Uri.Path("/v1/create")),
          body.compactPrint,
          _,
        )
      )
      _ = status shouldBe StatusCodes.ServiceUnavailable
      _ =
        output shouldBe "The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!"
      _ = proxy.toxics().get("timeout").remove()
      (status, _) <- headersWithParties(List(p.unwrap)).flatMap(
        postCreateCommand(
          accountCreateCommand(p, "25"),
          encoder,
          uri,
          _,
        )
      )
      _ = status shouldBe StatusCodes.OK
      // Server -> Client connection
      _ = proxy.toxics().timeout("timeout", ToxicDirection.DOWNSTREAM, 0)
      (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
        postJsonStringRequestEncoded(
          uri.withPath(Uri.Path("/v1/create")),
          body.compactPrint,
          _,
        )
      )
      _ = status shouldBe StatusCodes.ServiceUnavailable
      _ =
        output shouldBe "The server was not able to produce a timely response to your request.\r\nPlease try again in a short while!"
    } yield succeed
  }

  "/v1/query GET succeeds after reconnect" in withHttpService[Assertion] {
    (uri, encoder, _, client) =>
      for {
        p <- allocateParty(client, "Alice")
        (status, _) <- headersWithParties(List(p.unwrap)).flatMap(
          postCreateCommand(
            accountCreateCommand(p, "23"),
            encoder,
            uri,
            _,
          )
        )
        (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
          getRequest(
            uri = uri.withPath(Uri.Path("/v1/query")),
            _,
          )
        )
        _ <- inside(output) { case JsObject(fields) =>
          inside(fields.get("result")) { case Some(JsArray(rs)) =>
            rs.size shouldBe 1
          }
        }
        _ = proxy.disable()
        (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
          getRequest(
            uri = uri.withPath(Uri.Path("/v1/query")),
            _,
          )
        )
        _ <- inside(output) { case JsObject(fields) =>
          inside(fields.get("status")) { case Some(JsNumber(code)) =>
            code shouldBe 501
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
        (status, _) <- headersWithParties(List(p.unwrap)).flatMap(
          postCreateCommand(
            accountCreateCommand(p, "23"),
            encoder,
            uri,
            _,
          )
        )
        _ = status shouldBe StatusCodes.OK
        query = jsObject("""{"templateIds": ["Account:Account"]}""")
        (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
          postRequest(
            uri = uri.withPath(Uri.Path("/v1/query")),
            query,
            _,
          )
        )
        _ = status shouldBe StatusCodes.OK
        _ <- inside(output) { case JsObject(fields) =>
          inside(fields.get("result")) { case Some(JsArray(rs)) =>
            rs.size shouldBe 1
          }
        }
        _ = proxy.disable()
        (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
          postRequest(
            uri = uri.withPath(Uri.Path("/v1/query")),
            query,
            _,
          )
        )
        _ <- inside(output) { case JsObject(fields) =>
          inside(fields.get("status")) { case Some(JsNumber(code)) =>
            code shouldBe 501
          }
        }
        // TODO Document this properly or adjust it
        _ = status shouldBe StatusCodes.OK
        _ = proxy.enable()
        // eventually doesn’t handle Futures in the version of scalatest we’re using.
        _ <- RetryStrategy.constant(5, 2.seconds)((_, _) =>
          for {
            (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
              postRequest(
                uri = uri.withPath(Uri.Path("/v1/query")),
                query,
                _,
              )
            )
            _ = status shouldBe StatusCodes.OK
            _ <- inside(output) { case JsObject(fields) =>
              inside(fields.get("result")) { case Some(JsArray(rs)) =>
                rs.size shouldBe 1
              }
            }
          } yield succeed
        )
      } yield succeed
  }

  "/v1/query POST succeeds after reconnect to DB" in withHttpService { (uri, encoder, _, client) =>
    for {
      p <- allocateParty(client, "Alice")
      (status, _) <- headersWithParties(List(p.unwrap)).flatMap(
        postCreateCommand(
          accountCreateCommand(p, "23"),
          encoder,
          uri,
          _,
        )
      )
      _ = status shouldBe StatusCodes.OK
      query = jsObject("""{"templateIds": ["Account:Account"]}""")
      (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
        postRequest(
          uri = uri.withPath(Uri.Path("/v1/query")),
          query,
          _,
        )
      )
      _ = status shouldBe StatusCodes.OK
      _ <- inside(output) { case JsObject(fields) =>
        inside(fields.get("result")) { case Some(JsArray(rs)) =>
          rs.size shouldBe 1
        }
      }
      _ = dbProxy.disable()
      (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
        postRequest(
          uri = uri.withPath(Uri.Path("/v1/query")),
          query,
          _,
        )
      )
      _ <- inside(output) { case JsObject(fields) =>
        inside(fields.get("status")) { case Some(JsNumber(code)) =>
          code shouldBe 501
        }
      }
      // TODO Document this properly or adjust it
      _ = status shouldBe StatusCodes.OK
      (status, out) <- getRequestEncoded(uri.withPath(Uri.Path("/readyz")))
      _ = status shouldBe StatusCodes.ServiceUnavailable
      _ = out shouldBe
        """[+] ledger ok (SERVING)
          |[-] database failed
          |readyz check failed
          |""".stripMargin.replace("\r\n", "\n")
      _ = dbProxy.enable()
      // eventually doesn’t handle Futures in the version of scalatest we’re using.
      _ <- RetryStrategy.constant(5, 2.seconds)((_, _) =>
        for {
          (status, output) <- headersWithParties(List(p.unwrap)).flatMap(
            postRequest(
              uri = uri.withPath(Uri.Path("/v1/query")),
              query,
              _,
            )
          )
          _ = status shouldBe StatusCodes.OK
          _ <- inside(output) { case JsObject(fields) =>
            inside(fields.get("result")) { case Some(JsArray(rs)) =>
              rs.size shouldBe 1
            }
          }
        } yield succeed
      )
      (status, _) <- getRequestEncoded(uri.withPath(Uri.Path("/readyz")))
      _ = status shouldBe StatusCodes.OK
    } yield succeed
  }

  "/v1/stream/query can reconnect" in withHttpService { (uri, encoder, _, client) =>
    val query =
      """[
          {"templateIds": ["Account:Account"]}
        ]"""

    val offset = Promise[Offset]()

    def respBefore(accountCid: domain.ContractId): Sink[JsValue, Future[Unit]] = {
      val dslSyntax = Consume.syntax[JsValue]
      import dslSyntax._
      Consume.interpret(
        for {
          ContractDelta(Vector((ctId, _)), Vector(), None) <- readOne
          _ = ctId shouldBe accountCid.unwrap
          ContractDelta(Vector(), Vector(), Some(liveStartOffset)) <- readOne
          _ = offset.success(liveStartOffset)
          _ = proxy.disable()
          _ <- drain
        } yield ()
      )
    }

    def respAfter(
        offset: domain.Offset,
        accountCid: domain.ContractId,
        stop: UniqueKillSwitch,
    ): Sink[JsValue, Future[Unit]] = {
      val dslSyntax = Consume.syntax[JsValue]
      import dslSyntax._
      Consume.interpret(
        for {
          ContractDelta(Vector((ctId, _)), Vector(), Some(newOffset)) <- readOne
          _ = ctId shouldBe accountCid.unwrap
          _ = newOffset.unwrap should be > offset.unwrap
          _ = stop.shutdown()
          _ <- drain
        } yield ()
      )
    }

    for {
      p <- allocateParty(client, "p")
      (status, r) <- headersWithParties(List(p.unwrap)).flatMap(
        postCreateCommand(
          accountCreateCommand(p, "abc123"),
          encoder,
          uri,
          _,
        )
      )
      _ = status shouldBe a[StatusCodes.Success]
      cid = getContractId(getResult(r))
      jwt <- jwtForParties(uri)(List(p.unwrap), List(), ledgerId().unwrap)
      r <- (singleClientQueryStream(
        jwt,
        uri,
        query,
      ) via parseResp runWith respBefore(cid)).transform(x => Success(x))
      _ = inside(r) { case Failure(e: PeerClosedConnectionException) =>
        e.closeCode shouldBe 1011
        e.closeReason shouldBe "internal error"
      }
      offset <- offset.future
      _ = proxy.enable()
      (status, r) <- headersWithParties(List(p.unwrap)).flatMap(
        postCreateCommand(
          accountCreateCommand(p, "abc456"),
          encoder,
          uri,
          _,
        )
      )
      cid = getContractId(getResult(r))
      _ = status shouldBe a[StatusCodes.Success]
      jwt <- jwtForParties(uri)(List(p.unwrap), List(), ledgerId().unwrap)
      (stop, source) = singleClientQueryStream(
        jwt,
        uri,
        query,
        Some(offset),
      ).viaMat(KillSwitches.single)(Keep.right).preMaterialize()
      _ <- source via parseResp runWith respAfter(offset, cid, stop)
    } yield succeed

  }

  "fromStartupMode should not succeed for any input when the connection to the db is broken" in {
    import cats.effect.IO
    import DbStartupOps._, com.daml.http.dbbackend.DbStartupMode._,
    com.daml.http.dbbackend.JdbcConfig, com.daml.dbutils
    val bc = jdbcConfig_.baseConfig
    implicit val metrics: Metrics = new Metrics(new MetricRegistry())
    val dao = dbbackend.ContractDao(
      JdbcConfig(
        // discarding other settings
        dbutils.JdbcConfig(
          driver = bc.driver,
          url = bc.url,
          user = bc.user,
          password = bc.password,
          poolSize = ConnectionPool.PoolSize.Integration,
        )
      )
    )
    util.Logging
      .instanceUUIDLogCtx[IO[Assertion]](implicit lc =>
        for {
          _ <- IO(dbProxy.disable())
          res1 <- fromStartupMode(dao, CreateOnly)
          res2 <- fromStartupMode(dao, CreateAndStart)
          res3 <- fromStartupMode(dao, StartOnly)
          res4 <- fromStartupMode(dao, CreateIfNeededAndStart)
        } yield {
          res1 shouldBe false
          res2 shouldBe false
          res3 shouldBe false
          res4 shouldBe false
        }
      )
      .unsafeToFuture()
  }

  protected def jsObject(s: String): JsObject = {
    val r: JsonError \/ JsObject = for {
      jsVal <- SprayJson.parse(s).leftMap(e => JsonError(e.shows))
      jsObj <- SprayJson.mustBeJsObject(jsVal)
    } yield jsObj
    r.valueOr(e => fail(e.shows))
  }
}

// XXX (#13113 SC) these take about 70s each, with lots of I/O wait; if they stick around
// for a while (rather than deprecating/deleting custom token) might be worth
// splitting into a suite and consequently librifying the above in bazel

final class FailureTestsCustomToken extends FailureTests with HttpServiceUserFixture.CustomToken

final class FailureTestsUserToken extends FailureTests with HttpServiceUserFixture.UserToken
