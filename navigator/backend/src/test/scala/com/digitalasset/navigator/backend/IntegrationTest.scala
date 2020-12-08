// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.http.scaladsl.settings.ServerSettings
import akka.util.ByteString
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundAll}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.navigator.config.{Arguments, Config}
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.platform.sandbox.services.SandboxFixture
import com.daml.timer.RetryStrategy

import scala.concurrent.Future
import scala.concurrent.duration._

class IntegrationTest
    extends AsyncFreeSpec
    with SandboxFixture
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundAll
    with Matchers {
  self: Suite =>
  private def withNavigator[A](testFn: (Uri, LedgerClient) => Future[A]): Future[A] = {
    val args = Arguments(port = 0, participantPort = serverPort.value)
    val sys = ActorSystem("navigator")
    val (graphQL, info, _, getAppState, partyRefresh) = NavigatorBackend.setup(args, Config())(sys)
    val bindingF = Http()
      .newServerAt("localhost", 0)
      .withSettings(ServerSettings(system).withTransparentHeadRequests(true))
      .bind(NavigatorBackend.getRoute(system, args, graphQL, info, getAppState))
    val clientF = LedgerClient(
      channel,
      LedgerClientConfiguration(
        applicationId = "foobar",
        LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default,
        sslContext = None))
    val fa = for {
      binding <- bindingF
      client <- clientF
      uri = Uri.from(
        scheme = "http",
        host = binding.localAddress.getHostName,
        port = binding.localAddress.getPort)
      a <- testFn(uri, client)
    } yield a
    fa.transformWith { ta =>
      partyRefresh.foreach(_.cancel())
      Future
        .sequence(
          Seq[Future[Unit]](
            bindingF.flatMap(_.unbind()).map(_ => ()),
            sys.terminate().map(_ => ())))
        .transform(_ => ta)
    }
  }

  def getResponseDataBytes(resp: HttpResponse): Future[String] = {
    val fb = resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)
    fb
  }

  "Navigator" - {
    "picks up newly allocated parties" in withNavigator {
      case (uri, client) =>
        for {
          resp <- Http().singleRequest(HttpRequest(uri = uri.withPath(Uri.Path("/api/session/"))))
          respBody <- getResponseDataBytes(resp)
          _ = respBody shouldBe """{"method":{"type":"select","users":[]},"type":"sign-in"}"""
          _ = resp.status shouldBe StatusCodes.OK
          _ <- client.partyManagementClient
            .allocateParty(hint = None, displayName = Some("display-name"))
          _ <- RetryStrategy.constant(20, 1.second) {
            case (run @ _, duration @ _) =>
              for {
                resp <- Http().singleRequest(
                  HttpRequest(uri = uri.withPath(Uri.Path("/api/session/"))))
                respBody <- getResponseDataBytes(resp)
              } yield {
                respBody shouldBe """{"method":{"type":"select","users":["display-name"]},"type":"sign-in"}"""
              }
          }
        } yield succeed
    }
  }
}
