// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.settings.ServerSettings
import akka.util.ByteString
import com.daml.buildinfo.BuildInfo
import com.daml.integrationtest.CantonFixture
import com.daml.ledger.api.domain
import com.daml.ledger.client.LedgerClient
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.UserId
import com.daml.navigator.config.{Arguments, Config}
import org.scalatest._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.timer.RetryStrategy
import com.google.protobuf.field_mask.FieldMask

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class IntegrationTest extends AsyncFreeSpec with CantonFixture with Matchers {
  self: Suite =>

  private def withNavigator[A](
      userMgmt: Boolean
  )(testFn: Uri => LedgerClient => Future[A]): Future[A] = {
    import scala.jdk.FutureConverters
    val args = Arguments(
      port = 0,
      participantPort = ports.head.value,
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

    // TODO https://github.com/digital-asset/daml/issues/12663 participant user management: Emulating no-pagination
    def listAllUsers(client: LedgerClient) =
      client.userManagementClient.listUsers(pageToken = "", pageSize = 10000).map {
        case (users, _) => users.toList
      }

    // Don't close the LedgerClient on termination, because all it does is close the channel,
    // which then causes a subsequent test to fail when creating a LedgerClient using a closed channel
    // ("io.grpc.StatusRuntimeException: UNAVAILABLE: Channel shutdown invoked")
    sys.registerOnTermination {
      partyRefresh.foreach(_.cancel())
      Await.ready(bindingF.flatMap(_.terminate(30.seconds)), 30.seconds)
    }
    import FutureConverters._
    for {
      binding <- bindingF
      client <- defaultLedgerClient()
      uri = Uri.from(
        scheme = "http",
        host = binding.localAddress.getHostName,
        port = binding.localAddress.getPort,
      )
      a <- testFn(uri)(client)
      _ <- sys.terminate()
      _ <- Await.ready(sys.getWhenTerminated.asScala, 30.seconds)
      _ = logger.info(s"Terminated actor system ${sys.name}")
      // Reset sandbox enough to avoid users leaking between tests: delete all users except admin
      users <- listAllUsers(client)
      _ <- Future.traverse(users)(user =>
        if (user.id != "participant_admin") client.userManagementClient.deleteUser(user.id)
        else Future.unit
      )
      _ = logger.info("Removed all users from ledger as part of cleanup.")
    } yield a
  }

  private def okSessionBody(expectedBody: String, useRegex: Boolean = false)(implicit
      uri: Uri
  ): Future[Assertion] = {
    RetryStrategy.constant(20, 1.second) { case (_, _) =>
      for {
        resp <- Http().singleRequest(
          HttpRequest(uri = uri.withPath(Uri.Path("/api/session/")))
        )
        respBody <- resp.entity.dataBytes
          .runFold(ByteString.empty)((b, a) => b ++ a)
          .map(_.utf8String)
        _ = resp.status shouldBe StatusCodes.OK
      } yield
        if (useRegex)
          (respBody should fullyMatch regex expectedBody.r)
        else
          (respBody shouldBe expectedBody)
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

  private def updateUser(userName: String, primaryParty: Ref.Party)(implicit
      client: LedgerClient
  ): Future[domain.User] = {
    client.userManagementClient
      .updateUser(
        domain.User(UserId.assertFromString(userName), Some(primaryParty)),
        Some(FieldMask(Seq("primary_party"))),
      )
  }

  private def createUser(userName: String)(implicit client: LedgerClient): Future[domain.User] = {
    client.userManagementClient
      .createUser(
        domain.User(UserId.assertFromString(userName), None)
      )
  }

  "Navigator (parties)" - {
    "picks up newly allocated parties" in withNavigator(userMgmt = false) {
      implicit uri => implicit client =>
        for {
          _ <- okSessionBody(
            """\{"method":\{"type":"select","users":\["participant0::[a-f0-9]+"\]\},"type":"sign-in"\}""",
            true,
          )
          _ <- allocateParty("display-name")
          _ <- okSessionBody(
            """\{"method":\{"type":"select","users":\["participant0::[a-f0-9]+","display-name"\]\},"type":"sign-in"\}""",
            true,
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
          _ <- updateUser("user-name-2", partyDetails.party)
          _ <- okSessionBody(
            """{"method":{"type":"select","users":["user-name-1","user-name-2"]},"type":"sign-in"}"""
          )
        } yield succeed
    }

    "can only log in if user has a primary party (1 user, 0 primary parties)" in withNavigator(
      userMgmt = true
    ) { implicit uri => implicit client =>
      for {
        _ <- okSessionBody("""{"method":{"type":"select","users":[]},"type":"sign-in"}""")
        _ <- createUser("user-name-1")
        _ <- okSessionBody(
          """{"method":{"type":"select","users":[]},"type":"sign-in"}"""
        )
      } yield succeed
    }
  }

}
