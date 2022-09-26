// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.daml.fetchcontracts.domain.ContractTypeId.OptionalPkg
import com.daml.http.HttpServiceTestFixture.{UseTls, authorizationHeader, postRequest}
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.http.dbbackend.JdbcConfig
import com.daml.http.domain.UserDetails
import com.daml.http.json.JsonProtocol._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.api.domain.UserRight.{CanActAs, ParticipantAdmin}
import com.daml.ledger.api.v1.{value => v}
import com.daml.lf.data.Ref
import com.daml.platform.sandbox.{SandboxRequiringAuthorization, SandboxRequiringAuthorizationFuns}
import com.typesafe.scalalogging.StrictLogging
import org.scalatest.{Assertion, AsyncTestSuite, Inside}
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList
import scalaz.syntax.show._
import spray.json.JsValue

import scala.concurrent.{ExecutionContext, Future}
import scalaz.syntax.tag._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceIntegrationTestUserManagementNoAuth
    extends AbstractHttpServiceIntegrationTestTokenIndependent
    with AbstractHttpServiceIntegrationTestFuns
    with HttpServiceUserFixture.UserToken
    with SandboxRequiringAuthorizationFuns
    with StrictLogging {

  this: AsyncTestSuite with Matchers with Inside =>

  override def jwt(uri: Uri)(implicit ec: ExecutionContext): Future[Jwt] =
    jwtForParties(uri)(List("Alice"), List())

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
    ).toJson shouldBe
      List(
        Map("type" -> "CanActAs", "party" -> ham.unwrap),
        Map("type" -> "CanReadAs", "party" -> spam.unwrap),
        Map("type" -> "ParticipantAdmin"),
      ).toJson
  }

  "create IOU" - {
    // TEST_EVIDENCE: Authorization: create IOU should work with correct user rights
    "should work with correct user rights" in withHttpService { fixture =>
      import fixture.{encoder, client => ledgerClient}
      for {
        (alice, _) <- fixture.getUniquePartyAndAuthHeaders("Alice")
        command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice)
        input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
        user <- createUser(ledgerClient)(
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
          .parseResponse[domain.ActiveContract[JsValue]]
      } yield inside(response) { case domain.OkResponse(activeContract, _, StatusCodes.OK) =>
        assertActiveContract(activeContract)(command, encoder)
      }
    }

    // TEST_EVIDENCE: Authorization: create IOU should fail if user has no permission
    "should fail if user has no permission" in withHttpService { fixture =>
      import fixture.{encoder, client => ledgerClient}
      val alice = getUniqueParty("Alice")
      val bob = getUniqueParty("Bob")
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
      for {
        user <- createUser(ledgerClient)(
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

    // TEST_EVIDENCE: Authorization: create IOU should fail if overwritten actAs & readAs result in missing permission even if the user would have the rights
    "should fail if overwritten actAs & readAs result in missing permission even if the user would have the rights" in withHttpService {
      fixture =>
        import fixture.{encoder, client => ledgerClient}
        val alice = getUniqueParty("Alice")
        val bob = getUniqueParty("Bob")
        val meta = domain.CommandMeta(
          None,
          Some(NonEmptyList(bob)),
          None,
          submissionId = None,
          deduplicationPeriod = None,
        )
        val command: domain.CreateCommand[v.Record, OptionalPkg] =
          iouCreateCommand(alice, meta = Some(meta))
        val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
        for {
          user <- createUser(ledgerClient)(
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
    import fixture.{client => ledgerClient}
    import com.daml.http.json.JsonProtocol._
    for {
      user <- createUser(ledgerClient)(
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
    "POST yields user rights for a specific user" in withHttpServiceAndClient {
      (uri, _, _, ledgerClient, _) =>
        import spray.json._
        val alice = getUniqueParty("Alice")
        val bob = getUniqueParty("Bob")
        for {
          user <- createUser(ledgerClient)(
            Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
            initialRights = List(
              CanActAs(Ref.Party.assertFromString(alice.unwrap)),
              CanActAs(Ref.Party.assertFromString(bob.unwrap)),
            ),
          )
          response <- postRequest(
            uri.withPath(Uri.Path("/v1/user/rights")),
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
      import fixture.{client => ledgerClient}
      val alice = getUniqueParty("Alice")
      val bob = getUniqueParty("Bob")
      for {
        user <- createUser(ledgerClient)(
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
    "supports creating a user" in withHttpServiceAndClient { (uri, _, _, _, _) =>
      import spray.json._
      val alice = getUniqueParty("Alice")
      val createUserRequest = domain.CreateUserRequest(
        "nice.user2",
        Some(alice.unwrap),
        Some(
          List[domain.UserRight](
            domain.CanActAs(alice),
            domain.ParticipantAdmin,
          )
        ),
      )
      for {
        response <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
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
    val alice = getUniqueParty("Alice")
    val usernames =
      List("nice.username", "nice.username2", "nice.username3").map(getUniqueUserName)
    val createUserRequests = usernames.map(name =>
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
    for {
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
    "POST yields information about a specific user" in withHttpServiceAndClient {
      (uri, _, _, _, _) =>
        import spray.json._
        val alice = getUniqueParty("Alice")
        val createUserRequest = domain.CreateUserRequest(
          getUniqueUserName("nice.user"),
          Some(alice.unwrap),
          Some(
            List[domain.UserRight](
              domain.CanActAs(alice),
              domain.ParticipantAdmin,
            )
          ),
        )
        for {
          response1 <- postRequest(
            uri.withPath(Uri.Path("/v1/user/create")),
            createUserRequest.toJson,
            headers = headersWithAdminAuth,
          ).parseResponse[JsValue]
          _ <- inside(response1) { case domain.OkResponse(r, _, StatusCodes.OK) =>
            r shouldBe JsObject()
          }
          response2 <- postRequest(
            uri.withPath(Uri.Path(s"/v1/user")),
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
      val alice = getUniqueParty("Alice")
      val createUserRequest = domain.CreateUserRequest(
        getUniqueUserName("nice.user"),
        Some(alice.unwrap),
        Some(
          List[domain.UserRight](
            domain.CanActAs(alice),
            domain.ParticipantAdmin,
          )
        ),
      )
      for {
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
      val alice = getUniqueParty("Alice")
      val createUserRequest = domain.CreateUserRequest(
        getUniqueUserName("nice.user"),
        Some(alice.unwrap),
        Some(
          List[domain.UserRight](
            domain.CanActAs(alice),
            domain.ParticipantAdmin,
          )
        ),
      )
      for {
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

  "granting the user rights for a specific user should be possible via a POST to the user/rights/grant endpoint" in withHttpServiceAndClient {
    (uri, _, _, ledgerClient, _) =>
      import spray.json._
      val alice = getUniqueParty("Alice")
      val bob = getUniqueParty("Bob")
      for {
        user <- createUser(ledgerClient)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap))
          ),
        )
        response <- postRequest(
          uri.withPath(Uri.Path("/v1/user/rights/grant")),
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
          uri.withPath(Uri.Path("/v1/user/rights")),
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

  "revoking the user rights for a specific user should be possible via a POST to the user/rights/revoke endpoint" in withHttpServiceAndClient {
    (uri, _, _, ledgerClient, _) =>
      import spray.json._
      val alice = getUniqueParty("Alice")
      val bob = getUniqueParty("Bob")
      val charlie = getUniqueParty("Charlie")
      for {
        user <- createUser(ledgerClient)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap)),
            CanActAs(Ref.Party.assertFromString(bob.unwrap)),
            ParticipantAdmin,
          ),
        )
        response <- postRequest(
          uri.withPath(Uri.Path("/v1/user/rights/revoke")),
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
          uri.withPath(Uri.Path("/v1/user/rights")),
          domain.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        ).parseResponse[List[domain.UserRight]]
      } yield inside(response2) { case domain.OkResponse(urs, _, StatusCodes.OK) =>
        urs should contain theSameElementsAs List[domain.UserRight](domain.CanActAs(alice))
      }
  }

  // TEST_EVIDENCE: Performance: creating and listing 20K users should be possible
  "creating and listing 20K users should be possible" in withHttpService { fixture =>
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
                domain.CanActAs(p),
                domain.ParticipantAdmin,
              )
            ),
          )
        }
      }

    // Create users in chunks to avoid overloading the server
    // https://doc.akka.io/docs/akka-http/current/client-side/pool-overflow.html
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
              createUsers(remainingRequests)
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

class HttpServiceIntegrationTestUserManagement
    extends HttpServiceIntegrationTestUserManagementNoAuth
    with SandboxRequiringAuthorization
