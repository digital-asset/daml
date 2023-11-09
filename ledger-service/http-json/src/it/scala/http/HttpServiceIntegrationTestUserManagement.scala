// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import org.apache.pekko.http.scaladsl.model.headers.Authorization
import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}
import com.daml.http.HttpServiceTestFixture.{UseTls, authorizationHeader, postRequest}
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.http.dbbackend.JdbcConfig
import com.daml.http.domain.UserDetails
import com.daml.http.json.JsonProtocol._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.api.domain.UserRight.{CanActAs, ParticipantAdmin}
import com.daml.lf.data.Ref
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import com.daml.test.evidence.tag.Security.Attack
import org.scalatest.{Assertion, AsyncTestSuite, Inside}
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList
import scalaz.syntax.show._
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}
import scalaz.syntax.tag._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceIntegrationTestUserManagement
    extends AbstractHttpServiceIntegrationTestQueryStoreIndependent
    with AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken {

  this: AsyncTestSuite with Matchers with Inside =>

  override def jwt(uri: Uri)(implicit ec: ExecutionContext): Future[Jwt] =
    jwtForParties(uri)(domain.Party subst List("Alice"), List(), ledgerId = "")

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

  def headersWithUserAuth(userId: String): List[Authorization] =
    authorizationHeader(jwtForUser(userId))

  override def jdbcConfig: Option[JdbcConfig] = None

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def useTls: UseTls = UseTls.Tls

  override def wsConfig: Option[WebsocketConfig] = None

  "Json format of UserRight should be stable" in Future {
    import spray.json._
    val ham = getUniqueParty("ham")
    val spam = getUniqueParty("spam")
    List[domain.UserRight](
      domain.CanActAs(ham),
      domain.CanReadAs(spam),
      domain.ParticipantAdmin,
      domain.IdentityProviderAdmin,
    ).toJson shouldBe
      List(
        Map("type" -> "CanActAs", "party" -> ham.unwrap),
        Map("type" -> "CanReadAs", "party" -> spam.unwrap),
        Map("type" -> "ParticipantAdmin"),
        Map("type" -> "IdentityProviderAdmin"),
      ).toJson
  }

  "create IOU" - {
    "should work with correct user rights" taggedAs authorizationSecurity.setHappyCase(
      "A ledger client can create an IOU with correct user rights"
    ) in withHttpService { fixture =>
      import fixture.{encoder, client}
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
          .parseResponse[domain.ActiveContract.ResolvedCtTyId[JsValue]]
      } yield inside(response) { case domain.OkResponse(activeContract, _, StatusCodes.OK) =>
        assertActiveContract(activeContract)(command, encoder)
      }
    }

    "should fail if user has no permission" taggedAs authorizationSecurity.setAttack(
      Attack(
        "Ledger client",
        "tries to create an IOU without permission",
        "refuse request with BAD_REQUEST",
      )
    ) in withHttpService { fixture =>
      import fixture.{encoder, client}
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
      } yield inside(response) { case domain.ErrorResponse(_, _, StatusCodes.BadRequest, _) =>
        succeed
      }
    }

    "should fail if overwritten actAs & readAs result in missing permission even if the user would have the rights" taggedAs authorizationSecurity
      .setAttack(
        Attack(
          "Ledger client",
          "tries to create an IOU but overwritten actAs & readAs result in missing permission",
          "refuse request with BAD_REQUEST",
        )
      ) in withHttpService { fixture =>
      import fixture.{encoder, client}
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        (bob, _) <- fixture.getUniquePartyAndAuthHeaders("Bob")
        meta = domain.CommandMeta(
          None,
          Some(NonEmptyList(bob)),
          None,
          submissionId = None,
          deduplicationPeriod = None,
          disclosedContracts = None,
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
      } yield inside(response) { case domain.ErrorResponse(_, _, StatusCodes.BadRequest, _) =>
        succeed
      }
    }
  }

  "requesting the user id should be possible via the user endpoint" in withHttpService { fixture =>
    import com.daml.http.json.JsonProtocol._
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
      assertion <-
        inside(output) { case domain.OkResponse(result, _, StatusCodes.OK) =>
          result shouldEqual UserDetails(
            user.id,
            user.primaryParty: Option[String],
          )
        }
    } yield assertion
  }

  "user/rights endpoint" - {
    "POST yields user rights for a specific user" in withHttpService { fixture =>
      import spray.json._
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
          domain.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[domain.UserRight]]
      } yield inside(response) { case domain.OkResponse(result, _, StatusCodes.OK) =>
        result should contain theSameElementsAs
          List[domain.UserRight](
            domain.CanActAs(alice),
            domain.CanActAs(bob),
          )
      }
    }

    "GET yields user rights for the current user" in withHttpService { fixture =>
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
          .parseResponse[List[domain.UserRight]]
        assertion <-
          inside(output) { case domain.OkResponse(result, _, StatusCodes.OK) =>
            result should contain theSameElementsAs
              List[domain.UserRight](
                domain.CanActAs(alice),
                domain.CanActAs(bob),
              )
          }
      } yield assertion
    }
  }

  "user/create endpoint" - {
    "supports creating a user" in withHttpService { fixture =>
      import spray.json._
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        createUserRequest = domain.CreateUserRequest(
          "nice.user2",
          Some(alice.unwrap),
          Some(
            List[domain.UserRight](
              domain.CanActAs(alice),
              domain.ParticipantAdmin,
            )
          ),
        )
        response <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[JsValue]
      } yield inside(response) { case domain.OkResponse(r, _, StatusCodes.OK) =>
        r shouldBe JsObject()
      }
    }

    "supports creating a user with default values" in withHttpServiceAndClient {
      (uri, _, _, _, _) =>
        import spray.json._
        val username = getUniqueUserName("nice.user")
        for {
          (status, _) <- postRequest(
            uri.withPath(Uri.Path("/v1/user/create")),
            JsObject("userId" -> JsString(username)),
            headers = headersWithAdminAuth,
          )
          _ <- status shouldBe StatusCodes.OK
          response2 <- postRequest(
            uri.withPath(Uri.Path("/v1/user")),
            domain.GetUserRequest(username).toJson,
            headers = headersWithAdminAuth,
          ).parseResponse[UserDetails]
          _ <- inside(response2) { case domain.OkResponse(ud, _, StatusCodes.OK) =>
            ud shouldEqual UserDetails(username, None)
          }
          response3 <- postRequest(
            uri.withPath(Uri.Path("/v1/user/rights")),
            domain.ListUserRightsRequest(username).toJson,
            headers = headersWithAdminAuth,
          ).parseResponse[List[domain.UserRight]]
        } yield inside(response3) { case domain.OkResponse(List(), _, StatusCodes.OK) =>
          succeed
        }
    }
  }

  "getting all users should be possible via the users endpoint" in withHttpService { fixture =>
    import spray.json._
    import scalaz.std.scalaFuture._
    import scalaz.syntax.traverse._
    import scalaz.std.list._
    for {
      (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
      usernames =
        List("nice.username", "nice.username2", "nice.username3").map(getUniqueUserName)
      createUserRequests = usernames.map(name =>
        domain.CreateUserRequest(
          name,
          Some(alice.unwrap),
          Some(
            List[domain.UserRight](
              domain.CanActAs(alice),
              domain.ParticipantAdmin,
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
    } yield inside(result) { case domain.OkResponse(users, _, StatusCodes.OK) =>
      users.map(_.userId) should contain allElementsOf usernames
    }
  }

  "user endpoint" - {
    "POST yields information about a specific user" in withHttpService { fixture =>
      import spray.json._
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        createUserRequest = domain.CreateUserRequest(
          getUniqueUserName("nice.user"),
          Some(alice.unwrap),
          Some(
            List[domain.UserRight](
              domain.CanActAs(alice),
              domain.ParticipantAdmin,
            )
          ),
        )
        response1 <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[JsValue]
        _ <- inside(response1) { case domain.OkResponse(r, _, StatusCodes.OK) =>
          r shouldBe JsObject()
        }
        response2 <- postRequest(
          fixture.uri.withPath(Uri.Path(s"/v1/user")),
          domain.GetUserRequest(createUserRequest.userId).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[UserDetails]
      } yield inside(response2) { case domain.OkResponse(ud, _, StatusCodes.OK) =>
        ud shouldBe UserDetails(
          createUserRequest.userId,
          createUserRequest.primaryParty,
        )
      }
    }

    "GET yields information about the current user" in withHttpService { fixture =>
      import fixture.uri
      import spray.json._
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        createUserRequest = domain.CreateUserRequest(
          getUniqueUserName("nice.user"),
          Some(alice.unwrap),
          Some(
            List[domain.UserRight](
              domain.CanActAs(alice),
              domain.ParticipantAdmin,
            )
          ),
        )
        response1 <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[JsValue]
        _ <- inside(response1) { case domain.OkResponse(r, _, StatusCodes.OK) =>
          r shouldBe JsObject()
        }
        response2 <- fixture
          .getRequest(
            Uri.Path(s"/v1/user"),
            headers = headersWithUserAuth(createUserRequest.userId),
          )
          .parseResponse[UserDetails]
      } yield inside(response2) { case domain.OkResponse(userDetails, _, StatusCodes.OK) =>
        userDetails shouldBe UserDetails(
          createUserRequest.userId,
          createUserRequest.primaryParty,
        )
      }
    }
  }

  "deleting a specific user should be possible via the user/delete endpoint" in withHttpService {
    fixture =>
      import fixture.uri
      import spray.json._
      import spray.json.DefaultJsonProtocol._
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        createUserRequest = domain.CreateUserRequest(
          getUniqueUserName("nice.user"),
          Some(alice.unwrap),
          Some(
            List[domain.UserRight](
              domain.CanActAs(alice),
              domain.ParticipantAdmin,
            )
          ),
        )
        response1 <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[JsValue]
        _ <- inside(response1) { case domain.OkResponse(r, _, StatusCodes.OK) =>
          r shouldBe JsObject()
        }
        (status2, _) <- postRequest(
          uri.withPath(Uri.Path(s"/v1/user/delete")),
          domain.DeleteUserRequest(createUserRequest.userId).toJson,
          headers = headersWithAdminAuth,
        )
        _ = status2 shouldBe StatusCodes.OK
        response3 <- fixture
          .getRequest(
            Uri.Path("/v1/users"),
            headers = headersWithAdminAuth,
          )
          .parseResponse[List[UserDetails]]
      } yield inside(response3) { case domain.OkResponse(users, _, StatusCodes.OK) =>
        users should not contain createUserRequest.userId
      }
  }

  "granting the user rights for a specific user should be possible via a POST to the user/rights/grant endpoint" in withHttpService {
    fixture =>
      import spray.json._
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
          domain
            .GrantUserRightsRequest(
              user.id,
              List[domain.UserRight](
                domain.CanActAs(alice),
                domain.CanActAs(bob),
                domain.ParticipantAdmin,
              ),
            )
            .toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[domain.UserRight]]
        _ <- inside(response) { case domain.OkResponse(urs, _, StatusCodes.OK) =>
          urs should contain theSameElementsAs List[
            domain.UserRight
          ](domain.CanActAs(bob), domain.ParticipantAdmin)
        }
        response2 <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/rights")),
          domain.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[domain.UserRight]]
        assertion <- inside(response2) { case domain.OkResponse(urs, _, StatusCodes.OK) =>
          urs should contain theSameElementsAs
            List[domain.UserRight](
              domain.CanActAs(alice),
              domain.CanActAs(bob),
              domain.ParticipantAdmin,
            )
        }
      } yield assertion
  }

  "revoking the user rights for a specific user should be possible via a POST to the user/rights/revoke endpoint" in withHttpService {
    fixture =>
      import spray.json._
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
          domain
            .RevokeUserRightsRequest(
              user.id,
              List[domain.UserRight](
                domain.CanActAs(bob),
                domain.CanActAs(charlie),
                domain.ParticipantAdmin,
              ),
            )
            .toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[domain.UserRight]]
        _ <- inside(response) { case domain.OkResponse(urs, _, StatusCodes.OK) =>
          urs should contain theSameElementsAs List[domain.UserRight](
            domain.CanActAs(bob),
            domain.ParticipantAdmin,
          )
        }
        response2 <- postRequest(
          fixture.uri.withPath(Uri.Path("/v1/user/rights")),
          domain.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[domain.UserRight]]
      } yield inside(response2) { case domain.OkResponse(urs, _, StatusCodes.OK) =>
        urs should contain theSameElementsAs List[domain.UserRight](domain.CanActAs(alice))
      }
  }

  "creating and listing 20K users should be possible" taggedAs availabilitySecurity.setHappyCase(
    "A ledger client can create and list 20K users"
  ) in withHttpService { fixture =>
    import fixture.uri
    import spray.json._
    import spray.json.DefaultJsonProtocol._

    val createdUsers = 20000

    val createUserRequests: List[domain.CreateUserRequest] =
      List.tabulate(createdUsers) { sequenceNumber =>
        {
          val p = getUniqueParty(f"p$sequenceNumber%05d")
          domain.CreateUserRequest(
            p.unwrap,
            Some(p.unwrap),
            Some(
              List[domain.UserRight](
                domain.ParticipantAdmin
              )
            ),
          )
        }
      }

    // Create users in chunks to avoid overloading the server
    // https://doc.pekko.io/docs/pekko-http/current/client-side/pool-overflow.html
    def createUsers(
        createUserRequests: Seq[domain.CreateUserRequest],
        chunkSize: Int = 20,
    ): Future[Assertion] = {
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
    }

    for {
      _ <- createUsers(createUserRequests)
      response <- fixture
        .getRequest(
          Uri.Path("/v1/users"),
          headers = headersWithAdminAuth,
        )
        .parseResponse[List[UserDetails]]
    } yield inside(response) { case domain.OkResponse(users, _, StatusCodes.OK) =>
      val userIds = users.map(_.userId)
      val expectedUserIds = "participant_admin" :: createUserRequests.map(_.userId)
      userIds should contain allElementsOf expectedUserIds
    }
  }
}
