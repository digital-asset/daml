// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.settings.ServerSettings
import akka.util.ByteString
import com.daml.buildinfo.BuildInfo
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundAll}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.navigator.config.{Arguments, Config}
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.platform.sandbox.services.SandboxFixture
import com.daml.timer.RetryStrategy
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class IntegrationTest
    extends AsyncFreeSpec
    with SandboxFixture
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundAll
    with Matchers {
  self: Suite =>

  private val logger = LoggerFactory.getLogger(getClass)

  private def withNavigator[A](
      userMgmt: Boolean
  )(testFn: Uri => LedgerClient => Future[A]): Future[A] = {
    import scala.jdk.FutureConverters
    val args = Arguments(
      port = 0,
      participantPort = serverPort.value,
      enableUserManagement = userMgmt,
    )
    val sys = ActorSystem(s"navigator-${UUID.randomUUID().toString}")
    val backend = new UIBackend {
      override def customEndpoints: Set[CustomEndpoint[_]] = Set()
      override def customRoutes: List[Route] = Nil
      override def applicationInfo: ApplicationInfo = ApplicationInfo(
        id = s"Test-Navigator-${UUID.randomUUID().toString}",
        name = "Test-Navigator",
        version = BuildInfo.Version,
      )
    }

    val (graphQL, info, _, getAppState, partyRefresh) = backend.setup(args, Config())(sys)
    val bindingF = Http()
      .newServerAt("localhost", 0)
      .withSettings(ServerSettings(system).withTransparentHeadRequests(true))
      .bind(backend.getRoute(system, args, graphQL, info, getAppState))
    val clientF = LedgerClient(
      channel,
      LedgerClientConfiguration(
        applicationId = "foobar",
        LedgerIdRequirement.none,
        commandClient = CommandClientConfiguration.default,
      ),
    )
    import FutureConverters._
    import scalaz.syntax.traverse._
    import scalaz.std.list._
    import scalaz.std.scalaFuture._
    for {
      binding <- bindingF
      client <- clientF
      uri = Uri.from(
        scheme = "http",
        host = binding.localAddress.getHostName,
        port = binding.localAddress.getPort,
      )
      a <- testFn(uri)(client)
      _ <- Future(partyRefresh.foreach(_.cancel()))
      _ <- binding.unbind()
      _ <- binding.terminate(30.seconds)
      _ <- binding.whenTerminated
      _ <- sys.terminate()
      _ <- Await.ready(sys.getWhenTerminated.asScala, 30.seconds)
      _ = logger.info(s"Stopped actor system ${sys.name}")
      // Cleanup the users
      users <- client.userManagementClient.listUsers()
      _ <- users.toList.traverse(user =>
        if (user.id != "participant_admin") client.userManagementClient.deleteUser(user.id)
        else Future.unit
      )
      _ = logger.info(s"Removed all users from ledger as part of cleanup.")
    } yield a
//    fa.transformWith { ta =>

    // Don't close the LedgerClient because all it does is close the channel,
    // which then causes a subsequent test to fail when creating a LedgerClient using a closed channel
    // ("io.grpc.StatusRuntimeException: UNAVAILABLE: Channel shutdown invoked")
//      bindingF
//        .flatMap(_.unbind())
//        .flatMap(_ => sys.terminate())
//        .transform(_ => ta)
//    }
  }

  def getResponseDataBytes(resp: HttpResponse): Future[String] =
    resp.entity.dataBytes.runFold(ByteString.empty)((b, a) => b ++ a).map(_.utf8String)

  private def okSessionBody(expectedBody: String)(implicit uri: Uri): Future[Assertion] = {
    RetryStrategy.constant(20, 1.second) { case (_, _) =>
      for {
        resp <- Http().singleRequest(
          HttpRequest(uri = uri.withPath(Uri.Path("/api/session/")))
        )
        respBody <- getResponseDataBytes(resp)
        _ = resp.status shouldBe StatusCodes.OK
      } yield {
        respBody shouldBe expectedBody
      }
    }
  }

  private def allocateParty(partyName: String)(implicit client: LedgerClient) = {
    client.partyManagementClient
      .allocateParty(hint = None, displayName = Some(partyName))
  }

  private def createUser(userName: String, primaryParty: Ref.Party)(implicit
      client: LedgerClient
  ): Future[domain.User] = {
    client.userManagementClient
      .createUser(
        domain.User(UserId.assertFromString(userName), Some(primaryParty))
      )
  }

  "Navigator (parties)" - {
    "picks up newly allocated parties" in withNavigator(userMgmt = false) {
      implicit uri => implicit client =>
        for {
          _ <- okSessionBody("""{"method":{"type":"select","users":[]},"type":"sign-in"}""")
          _ <- allocateParty("display-name")
          _ <- okSessionBody(
            """{"method":{"type":"select","users":["display-name"]},"type":"sign-in"}"""
          )
        } yield succeed
    }
  }

  "Navigator (basic user)" - {
    "picks up newly created users (1 user)" in withNavigator(userMgmt = true) {
      implicit uri => implicit client =>
        for {
          _ <- okSessionBody("""{"method":{"type":"select","users":[]},"type":"sign-in"}""")
          partyDetails <- allocateParty("primary-party")
          _ <- createUser("user-name", partyDetails.party)
          _ <- okSessionBody(
            """{"method":{"type":"select","users":["user-name"]},"type":"sign-in"}"""
          )
        } yield succeed
    }
  }

  "Navigator (users)" - {
    "picks up newly created users (2 users, 1 primary party)" in withNavigator(userMgmt = true) {
      implicit uri => implicit client =>
        for {
          _ <- okSessionBody("""{"method":{"type":"select","users":[]},"type":"sign-in"}""")
          partyDetails <- allocateParty("primary-party")
          _ <- createUser("user-name-1", partyDetails.party)
          _ <- createUser("user-name-2", partyDetails.party)
          _ <- okSessionBody(
            """{"method":{"type":"select","users":["user-name-1","user-name-2"]},"type":"sign-in"}"""
          )
        } yield succeed
    }
  }

  "Navigator (users)" - {
    "picks up newly created users (2 users, 2 primary parties)" in withNavigator(userMgmt = true) {
      implicit uri => implicit client =>
        for {
          _ <- okSessionBody("""{"method":{"type":"select","users":[]},"type":"sign-in"}""")
          partyDetails <- allocateParty("primary-party")
          _ <- createUser("user-name-1", partyDetails.party)
          partyDetails2 <- allocateParty("primary-party2")
          _ <- createUser("user-name-2", partyDetails2.party)
          _ <- okSessionBody(
            """{"method":{"type":"select","users":["user-name-1","user-name-2"]},"type":"sign-in"}"""
          )
        } yield succeed
    }
  }

}
