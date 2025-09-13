// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.Attack
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.http
import com.digitalasset.canton.http.UserDetails
import com.digitalasset.canton.http.json.JsonProtocol.*
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.jsonapi.HttpServiceTestFixture.{
  UseTls,
  authorizationHeader,
}
import com.digitalasset.canton.ledger.api.UserRight.{CanActAs, ParticipantAdmin}
import com.digitalasset.canton.ledger.api.{User, UserRight}
import com.digitalasset.canton.ledger.client.LedgerClient as DamlLedgerClient
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.http.scaladsl.model.{HttpHeader, StatusCodes, Uri}
import org.scalatest.Assertion
import org.scalatest.time.{Millis, Seconds, Span}
import scalaz.NonEmptyList
import scalaz.syntax.show.*
import scalaz.syntax.tag.*
import spray.json.JsValue

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceIntegrationTestUserManagement
    extends AbstractHttpServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  // This high patience timeout is needed for the test case creating and listing 20K users
  implicit override val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(300, Seconds)), interval = scaled(Span(150, Millis)))

  def createUser(ledgerClient: DamlLedgerClient)(
      userId: Ref.UserId,
      primaryParty: Option[Ref.Party] = None,
      initialRights: List[UserRight] = List.empty,
  ): Future[User] =
    ledgerClient.userManagementClient.createUser(
      User(userId, primaryParty),
      initialRights,
      Some(jwtAdminNoParty.value),
    )

  def headersWithUserAuth(userId: String): List[HttpHeader] =
    authorizationHeader(jwtForUser(userId))

  override def useTls: UseTls = UseTls.Tls

  "Json format of UserRight" should {
    "be stable" in { _ =>
      import spray.json.*
      val ham = getUniqueParty("ham")
      val spam = getUniqueParty("spam")
      val clam = getUniqueParty("clam")
      List[http.UserRight](
        http.CanActAs(ham),
        http.CanReadAs(spam),
        http.CanExecuteAs(clam),
        http.ParticipantAdmin,
        http.IdentityProviderAdmin,
        http.CanReadAsAnyParty,
        http.CanExecuteAsAnyParty,
      ).toJson shouldBe
        List(
          Map("type" -> "CanActAs", "party" -> ham.unwrap),
          Map("type" -> "CanReadAs", "party" -> spam.unwrap),
          Map("type" -> "CanExecuteAs", "party" -> clam.unwrap),
          Map("type" -> "ParticipantAdmin"),
          Map("type" -> "IdentityProviderAdmin"),
          Map("type" -> "CanReadAsAnyParty"),
          Map("type" -> "CanExecuteAsAnyParty"),
        ).toJson
    }
  }

  "create IOU" should {
    "work with correct user rights" taggedAs authorizationSecurity.setHappyCase(
      "A ledger client can create an IOU with correct user rights"
    ) in withHttpService() { fixture =>
      import fixture.{client, encoder}

      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        command = iouCreateCommand(alice)
        input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
        user <- createUser(client)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap))
          ),
        )
        response <- fixture
          .postJsonRequest(
            Uri.Path("/v1/create"),
            input,
            headers = headersWithUserAuth(user.id),
          )
          .parseResponse[http.ActiveContract.ResolvedCtTyId[JsValue]]
      } yield inside(response) { case http.OkResponse(activeContract, _, StatusCodes.OK) =>
        assertActiveContract(activeContract)(command, encoder)
      }
    }

    "fail if user has no permission" taggedAs authorizationSecurity.setAttack(
      Attack(
        "Ledger client",
        "tries to create an IOU without permission",
        "refuse request with BAD_REQUEST",
      )
    ) in withHttpService() { fixture =>
      import fixture.{client, encoder}
      val alice = getUniqueParty("Alice")
      val command = iouCreateCommand(alice)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
      for {
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        user <- createUser(client)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(bob.unwrap))
          ),
        )
        response <- fixture
          .postJsonRequest(
            Uri.Path("/v1/create"),
            input,
            headers = headersWithUserAuth(user.id),
          )
          .parseResponse[JsValue]
      } yield inside(response) { case http.ErrorResponse(_, _, StatusCodes.BadRequest, _) =>
        succeed
      }
    }

    "fail if overwritten actAs & readAs result in missing permission even if the user would have the rights" taggedAs authorizationSecurity
      .setAttack(
        Attack(
          "Ledger client",
          "tries to create an IOU but overwritten actAs & readAs result in missing permission",
          "refuse request with BAD_REQUEST",
        )
      ) in withHttpService() { fixture =>
      import fixture.{client, encoder}
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        meta = http.CommandMeta(
          None,
          Some(NonEmptyList(bob)),
          None,
          submissionId = None,
          workflowId = None,
          deduplicationPeriod = None,
          disclosedContracts = None,
          synchronizerId = None,
          packageIdSelectionPreference = None,
        )
        command = iouCreateCommand(alice, meta = Some(meta))
        input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
        user <- createUser(client)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap)),
            CanActAs(Ref.Party.assertFromString(bob.unwrap)),
          ),
        )
        response <- fixture
          .postJsonRequest(
            Uri.Path("/v1/create"),
            input,
            headers = headersWithUserAuth(user.id),
          )
          .parseResponse[JsValue]
      } yield inside(response) { case http.ErrorResponse(_, _, StatusCodes.BadRequest, _) =>
        succeed
      }
    }
  }

  "requesting the user id" should {
    "be possible via the user endpoint" in withHttpService() { fixture =>
      for {
        user <- createUser(fixture.client)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List.empty,
        )
        output <- fixture
          .getRequest(
            Uri.Path("/v1/user"),
            headers = headersWithUserAuth(user.id),
          )
          .parseResponse[UserDetails]
      } yield {
        inside(output) { case http.OkResponse(result, _, StatusCodes.OK) =>
          result shouldEqual UserDetails(
            user.id,
            user.primaryParty: Option[String],
          )
        }
      }
    }
  }

  "user/rights endpoint" should {
    "POST yields user rights for a specific user" in withHttpService() { fixture =>
      import spray.json.*
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        user <- createUser(fixture.client)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap)),
            CanActAs(Ref.Party.assertFromString(bob.unwrap)),
          ),
        )
        response <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/rights")),
          http.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[http.UserRight]]
      } yield inside(response) { case http.OkResponse(result, _, StatusCodes.OK) =>
        result should contain theSameElementsAs
          List[http.UserRight](
            http.CanActAs(alice),
            http.CanActAs(bob),
          )
      }
    }

    "GET yields user rights for the current user" in withHttpService() { fixture =>
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        user <- createUser(fixture.client)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap)),
            CanActAs(Ref.Party.assertFromString(bob.unwrap)),
          ),
        )
        output <- fixture
          .getRequest(
            Uri.Path("/v1/user/rights"),
            headers = headersWithUserAuth(user.id),
          )
          .parseResponse[List[http.UserRight]]
      } yield {
        inside(output) { case http.OkResponse(result, _, StatusCodes.OK) =>
          result should contain theSameElementsAs
            List[http.UserRight](
              http.CanActAs(alice),
              http.CanActAs(bob),
            )
        }
      }
    }
  }

  "user/create endpoint" should {
    "support creating a user" in withHttpService() { fixture =>
      import spray.json.*
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        createUserRequest = http.CreateUserRequest(
          "nice.user2",
          Some(alice.unwrap),
          Some(
            List[http.UserRight](
              http.CanActAs(alice),
              http.ParticipantAdmin,
            )
          ),
        )
        response <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[JsValue]
      } yield inside(response) { case http.OkResponse(r, _, StatusCodes.OK) =>
        r shouldBe JsObject()
      }
    }

    "support creating a user with default values" in withHttpServiceAndClient { (uri, _, _, _) =>
      import spray.json.*
      val username = getUniqueUserName("nice.user")
      for {
        (status, _) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          JsObject("userId" -> JsString(username)),
          headers = headersWithAdminAuth,
        )
        _ = status shouldBe StatusCodes.OK
        response2 <- postRequest(
          uri.withPath(Uri.Path("/v1/user")),
          http.GetUserRequest(username).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[UserDetails]
        _ = inside(response2) { case http.OkResponse(ud, _, StatusCodes.OK) =>
          ud shouldEqual UserDetails(username, None)
        }
        response3 <- postRequest(
          uri.withPath(Uri.Path("/v1/user/rights")),
          http.ListUserRightsRequest(username).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[http.UserRight]]
      } yield inside(response3) { case http.OkResponse(List(), _, StatusCodes.OK) =>
        succeed
      }
    }
  }

  "be possible via the users endpoint to get all users" in withHttpService() { fixture =>
    import scalaz.std.list.*
    import scalaz.std.scalaFuture.*
    import scalaz.syntax.traverse.*
    import spray.json.*
    for {
      (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
      usernames =
        List("nice.username", "nice.username2", "nice.username3").map(getUniqueUserName)
      createUserRequests = usernames.map(name =>
        http.CreateUserRequest(
          name,
          Some(alice.unwrap),
          Some(
            List[http.UserRight](
              http.CanActAs(alice),
              http.ParticipantAdmin,
            )
          ),
        )
      )
      _ <- createUserRequests.traverse(createUserRequest =>
        for {
          (status, _) <- postRequest(
            fixture.uri.withPath(Uri.Path("/v1/user/create")),
            createUserRequest.toJson,
            headers = headersWithAdminAuth,
          )
          _ = status shouldBe StatusCodes.OK
        } yield ()
      )
      result <- fixture
        .getRequest(
          Uri.Path("/v1/users"),
          headers = headersWithAdminAuth,
        )
        .parseResponse[List[UserDetails]]
    } yield inside(result) { case http.OkResponse(users, _, StatusCodes.OK) =>
      users.map(_.userId) should contain allElementsOf usernames
    }
  }

  "user endpoint" should {
    "POST yields information about a specific user" in withHttpService() { fixture =>
      import spray.json.*
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        createUserRequest = http.CreateUserRequest(
          getUniqueUserName("nice.user"),
          Some(alice.unwrap),
          Some(
            List[http.UserRight](
              http.CanActAs(alice),
              http.ParticipantAdmin,
            )
          ),
        )
        response1 <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[JsValue]
        _ = inside(response1) { case http.OkResponse(r, _, StatusCodes.OK) =>
          r shouldBe JsObject()
        }
        response2 <- postRequest(
          fixture.uri.withPath(Uri.Path(s"/v1/user")),
          http.GetUserRequest(createUserRequest.userId).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[UserDetails]
      } yield inside(response2) { case http.OkResponse(ud, _, StatusCodes.OK) =>
        ud shouldBe UserDetails(
          createUserRequest.userId,
          createUserRequest.primaryParty,
        )
      }
    }

    "GET yields information about the current user" in withHttpService() { fixture =>
      import fixture.uri
      import spray.json.*
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        createUserRequest = http.CreateUserRequest(
          getUniqueUserName("nice.user"),
          Some(alice.unwrap),
          Some(
            List[http.UserRight](
              http.CanActAs(alice),
              http.ParticipantAdmin,
            )
          ),
        )
        response1 <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[JsValue]
        _ = inside(response1) { case http.OkResponse(r, _, StatusCodes.OK) =>
          r shouldBe JsObject()
        }
        response2 <- fixture
          .getRequest(
            Uri.Path(s"/v1/user"),
            headers = headersWithUserAuth(createUserRequest.userId),
          )
          .parseResponse[UserDetails]
      } yield inside(response2) { case http.OkResponse(usrDetails, _, StatusCodes.OK) =>
        usrDetails shouldBe UserDetails(
          createUserRequest.userId,
          createUserRequest.primaryParty,
        )
      }
    }
  }

  "deleting a specific user" should {
    "be possible via the user/delete endpoint" in withHttpService() { fixture =>
      import fixture.uri
      import spray.json.*
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        createUserRequest = http.CreateUserRequest(
          getUniqueUserName("nice.user"),
          Some(alice.unwrap),
          Some(
            List[http.UserRight](
              http.CanActAs(alice),
              http.ParticipantAdmin,
            )
          ),
        )
        response1 <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[JsValue]
        _ = inside(response1) { case http.OkResponse(r, _, StatusCodes.OK) =>
          r shouldBe JsObject()
        }
        (status2, _) <- postRequest(
          uri.withPath(Uri.Path(s"/v1/user/delete")),
          http.DeleteUserRequest(createUserRequest.userId).toJson,
          headers = headersWithAdminAuth,
        )
        _ = status2 shouldBe StatusCodes.OK
        response3 <- fixture
          .getRequest(
            Uri.Path("/v1/users"),
            headers = headersWithAdminAuth,
          )
          .parseResponse[List[UserDetails]]
      } yield inside(response3) { case http.OkResponse(users, _, StatusCodes.OK) =>
        users should not contain createUserRequest.userId
      }
    }
  }

  "granting the user rights for a specific user" should {
    "be possible via a POST to the user/rights/grant endpoint" in withHttpService() { fixture =>
      import spray.json.*
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        user <- createUser(fixture.client)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap))
          ),
        )
        response <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/rights/grant")),
          http
            .GrantUserRightsRequest(
              user.id,
              List[http.UserRight](
                http.CanActAs(alice),
                http.CanActAs(bob),
                http.ParticipantAdmin,
              ),
            )
            .toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[http.UserRight]]
        _ = inside(response) { case http.OkResponse(urs, _, StatusCodes.OK) =>
          urs should contain theSameElementsAs List[
            http.UserRight
          ](http.CanActAs(bob), http.ParticipantAdmin)
        }
        response2 <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/rights")),
          http.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[http.UserRight]]
        assertion = inside(response2) { case http.OkResponse(urs, _, StatusCodes.OK) =>
          urs should contain theSameElementsAs
            List[http.UserRight](
              http.CanActAs(alice),
              http.CanActAs(bob),
              http.ParticipantAdmin,
            )
        }
      } yield assertion
    }
  }

  "revoking the user rights for a specific user" should {
    "be possible via a POST to the user/rights/revoke endpoint" in withHttpService() { fixture =>
      import spray.json.*
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        (charlie, _) <- fixture.getUniquePartyAndAuthHeaders("Charlie")
        user <- createUser(fixture.client)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap)),
            CanActAs(Ref.Party.assertFromString(bob.unwrap)),
            ParticipantAdmin,
          ),
        )
        response <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/rights/revoke")),
          http
            .RevokeUserRightsRequest(
              user.id,
              List[http.UserRight](
                http.CanActAs(bob),
                http.CanActAs(charlie),
                http.ParticipantAdmin,
              ),
            )
            .toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[http.UserRight]]
        _ = inside(response) { case http.OkResponse(urs, _, StatusCodes.OK) =>
          urs should contain theSameElementsAs List[http.UserRight](
            http.CanActAs(bob),
            http.ParticipantAdmin,
          )
        }
        response2 <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/rights")),
          http.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[http.UserRight]]
      } yield inside(response2) { case http.OkResponse(urs, _, StatusCodes.OK) =>
        urs should contain theSameElementsAs List[http.UserRight](http.CanActAs(alice))
      }
    }
  }

  "creating and listing 20K users" should {
    "be possible" taggedAs availabilitySecurity.setHappyCase(
      "A ledger client can create and list 20K users"
    ) in withHttpService() { fixture =>
      import fixture.uri
      import spray.json.*

      val createdUsers = 20000

      val createUserRequests: List[http.CreateUserRequest] =
        List.tabulate(createdUsers) { sequenceNumber =>
          val p = getUniqueParty(f"p$sequenceNumber%05d")
          http.CreateUserRequest(
            p.unwrap,
            Some(p.unwrap),
            Some(
              List[http.UserRight](
                http.ParticipantAdmin
              )
            ),
          )
        }

      // Create users in chunks to avoid overloading the server
      // https://doc.akka.io/docs/akka-http/current/client-side/pool-overflow.html
      def createUsers(
          createUserRequests: Seq[http.CreateUserRequest],
          chunkSize: Int = 20,
      ): Future[Assertion] =
        createUserRequests.splitAt(chunkSize) match {
          case (Nil, _) => Future.successful(succeed)
          case (next, remainingRequests) =>
            Future
              .sequence {
                next.map { request =>
                  postRequest(
                    uri.withPath(Uri.Path("/v1/user/create")),
                    request.toJson,
                    headers = headersWithAdminAuth,
                  ).map(_._1)
                }
              }
              .flatMap { statusCodes =>
                all(statusCodes) shouldBe StatusCodes.OK
                createUsers(remainingRequests, chunkSize)
              }
        }

      for {
        _ <- createUsers(createUserRequests)
        response <- fixture
          .getRequest(
            Uri.Path("/v1/users"),
            headers = headersWithAdminAuth,
          )
          .parseResponse[List[UserDetails]]
      } yield inside(response) { case http.OkResponse(users, _, StatusCodes.OK) =>
        val userIds = users.map(_.userId)
        val expectedUserIds = "participant_admin" :: createUserRequests.map(_.userId)
        userIds should contain allElementsOf expectedUserIds
      }
    }
  }

}
