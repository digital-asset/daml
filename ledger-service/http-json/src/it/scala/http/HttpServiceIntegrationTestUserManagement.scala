// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.daml.fetchcontracts.domain.TemplateId.OptionalPkg
import com.daml.http.HttpServiceTestFixture.{UseTls, authorizationHeader, getResult, postRequest}
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

  "create IOU should work with correct user rights" in withHttpServiceAndClient {
    (uri, encoder, _, ledgerClient, _) =>
      val alice = getUniqueParty("Alice")
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
      for {
        user <- createUser(ledgerClient)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap))
          ),
        )
        (status, output) <- postJsonRequest(
          uri.withPath(Uri.Path("/v1/create")),
          input,
          headers = headersWithUserAuth(user.id),
        )
        assertion <- {
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          val activeContract = getResult(output)
          assertActiveContract(activeContract)(command, encoder)
        }
      } yield assertion
  }

  "create IOU should fail if user has no permission" in withHttpServiceAndClient {
    (uri, encoder, _, ledgerClient, _) =>
      val alice = getUniqueParty("Alice")
      val bob = getUniqueParty("Bob")
      val command: domain.CreateCommand[v.Record, OptionalPkg] = iouCreateCommand(alice.unwrap)
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
      for {
        user <- createUser(ledgerClient)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(bob.unwrap))
          ),
        )
        (status, output) <- postJsonRequest(
          uri.withPath(Uri.Path("/v1/create")),
          input,
          headers = headersWithUserAuth(user.id),
        )
        assertion <- {
          status shouldBe StatusCodes.BadRequest
          assertStatus(output, StatusCodes.BadRequest)
        }
      } yield assertion
  }

  "create IOU should fail if overwritten actAs & readAs result in missing permission even if the user would have the rights" in withHttpServiceAndClient {
    (uri, encoder, _, ledgerClient, _) =>
      val alice = getUniqueParty("Alice")
      val bob = getUniqueParty("Bob")
      val meta = domain.CommandMeta(None, Some(NonEmptyList(bob)), None)
      val command: domain.CreateCommand[v.Record, OptionalPkg] =
        iouCreateCommand(alice.unwrap, meta = Some(meta))
      val input: JsValue = encoder.encodeCreateCommand(command).valueOr(e => fail(e.shows))
      for {
        user <- createUser(ledgerClient)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List(
            CanActAs(Ref.Party.assertFromString(alice.unwrap)),
            CanActAs(Ref.Party.assertFromString(bob.unwrap)),
          ),
        )
        (status, output) <- postJsonRequest(
          uri.withPath(Uri.Path("/v1/create")),
          input,
          headers = headersWithUserAuth(user.id),
        )
        assertion <- {
          status shouldBe StatusCodes.BadRequest
          assertStatus(output, StatusCodes.BadRequest)
        }
      } yield assertion
  }

  "requesting the user id should be possible via the user endpoint" in withHttpServiceAndClient {
    (uri, _, _, ledgerClient, _) =>
      import com.daml.http.json.JsonProtocol._
      for {
        user <- createUser(ledgerClient)(
          Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
          initialRights = List.empty,
        )
        (status, output) <- getRequest(
          uri.withPath(Uri.Path("/v1/user")),
          headers = headersWithUserAuth(user.id),
        )
        assertion <- {
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          getResult(output).convertTo[UserDetails] shouldEqual UserDetails(
            user.id,
            user.primaryParty: Option[String],
          )
        }
      } yield assertion
  }

  "requesting the user rights for a specific user should be possible via a POST to the user/rights endpoint" in withHttpServiceAndClient {
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
        (status, output) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/rights")),
          domain.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        )
        assertion <- {
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          getResult(output).convertTo[List[domain.UserRight]] should contain theSameElementsAs
            List[domain.UserRight](
              domain.CanActAs(alice),
              domain.CanActAs(bob),
            )
        }
      } yield assertion
  }

  "requesting the user rights for the current user should be possible via a GET to the user/rights endpoint" in withHttpServiceAndClient {
    (uri, _, _, ledgerClient, _) =>
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
        (status, output) <- getRequest(
          uri.withPath(Uri.Path("/v1/user/rights")),
          headers = headersWithUserAuth(user.id),
        )
        assertion <- {
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          getResult(output).convertTo[List[domain.UserRight]] should contain theSameElementsAs
            List[domain.UserRight](
              domain.CanActAs(alice),
              domain.CanActAs(bob),
            )
        }
      } yield assertion
  }

  "creating a user should be possible via the user/create endpoint" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
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
        (status, output) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        )
      } yield {
        status shouldBe StatusCodes.OK
        getResult(output) shouldBe JsObject()
      }
  }

  "creating a user with default values should be possible via the user/create endpoint" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
      import spray.json._
      val username = getUniqueUserName("nice.user")
      for {
        (status, output) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          JsObject("userId" -> JsString(username)),
          headers = headersWithAdminAuth,
        )
        _ <- status shouldBe StatusCodes.OK
        (status2, output2) <- postRequest(
          uri.withPath(Uri.Path("/v1/user")),
          domain.GetUserRequest(username).toJson,
          headers = headersWithAdminAuth,
        )
        _ <- status2 shouldBe StatusCodes.OK
        _ <- getResult(output2).convertTo[UserDetails] shouldEqual UserDetails(username, None)
        (status3, output3) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/rights")),
          domain.ListUserRightsRequest(username).toJson,
          headers = headersWithAdminAuth,
        )
        _ <- status3 shouldBe StatusCodes.OK
      } yield getResult(output3)
        .convertTo[List[domain.UserRight]] shouldEqual List.empty
  }

  "getting all users should be possible via the users endpoint" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
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
              uri.withPath(Uri.Path("/v1/user/create")),
              createUserRequest.toJson,
              headers = headersWithAdminAuth,
            )
            _ = status shouldBe StatusCodes.OK
          } yield ()
        )
        (status, output) <- getRequest(
          uri.withPath(Uri.Path("/v1/users")),
          headers = headersWithAdminAuth,
        )
        _ = status shouldBe StatusCodes.OK
        users = getResult(output).convertTo[List[UserDetails]]
      } yield users.map(_.userId) should contain allElementsOf usernames
  }

  "getting information about a specific user should be possible via the user endpoint" in withHttpServiceAndClient {
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
        (status1, output1) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        )
        _ <- {
          status1 shouldBe StatusCodes.OK
          getResult(output1) shouldBe JsObject()
        }
        (status2, output2) <- postRequest(
          uri.withPath(Uri.Path(s"/v1/user")),
          domain.GetUserRequest(createUserRequest.userId).toJson,
          headers = headersWithAdminAuth,
        )
      } yield {
        status2 shouldBe StatusCodes.OK
        getResult(output2).convertTo[UserDetails] shouldBe UserDetails(
          createUserRequest.userId,
          createUserRequest.primaryParty,
        )
      }
  }

  "getting information about the current user should be possible via the user endpoint" in withHttpServiceAndClient {
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
        (status1, output1) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        )
        _ <- {
          status1 shouldBe StatusCodes.OK
          getResult(output1) shouldBe JsObject()
        }
        (status2, output2) <- getRequest(
          uri.withPath(Uri.Path(s"/v1/user")),
          headers = headersWithUserAuth(createUserRequest.userId),
        )
      } yield {
        status2 shouldBe StatusCodes.OK
        getResult(output2).convertTo[UserDetails] shouldBe UserDetails(
          createUserRequest.userId,
          createUserRequest.primaryParty,
        )
      }
  }

  "deleting a specific user should be possible via the user/delete endpoint" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
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
        (status1, output1) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/create")),
          createUserRequest.toJson,
          headers = headersWithAdminAuth,
        )
        _ <- {
          status1 shouldBe StatusCodes.OK
          getResult(output1) shouldBe JsObject()
        }
        (status2, _) <- postRequest(
          uri.withPath(Uri.Path(s"/v1/user/delete")),
          domain.DeleteUserRequest(createUserRequest.userId).toJson,
          headers = headersWithAdminAuth,
        )
        _ = status2 shouldBe StatusCodes.OK
        (status3, output3) <- getRequest(
          uri.withPath(Uri.Path("/v1/users")),
          headers = headersWithAdminAuth,
        )
      } yield {
        status3 shouldBe StatusCodes.OK
        getResult(output3).convertTo[List[UserDetails]] should not contain createUserRequest.userId
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
        (status, output) <- postRequest(
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
        )
        _ <- {
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          getResult(output).convertTo[List[domain.UserRight]] should contain theSameElementsAs List[
            domain.UserRight
          ](domain.CanActAs(bob), domain.ParticipantAdmin)
        }
        (status2, output2) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/rights")),
          domain.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        )
        assertion <- {
          status2 shouldBe StatusCodes.OK
          assertStatus(output2, StatusCodes.OK)
          getResult(output2).convertTo[List[domain.UserRight]] should contain theSameElementsAs
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
        (status, output) <- postRequest(
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
        )
        _ <- {
          status shouldBe StatusCodes.OK
          assertStatus(output, StatusCodes.OK)
          getResult(output)
            .convertTo[List[domain.UserRight]] should contain theSameElementsAs List[
            domain.UserRight
          ](
            domain.CanActAs(bob),
            domain.ParticipantAdmin,
          )
        }
        (status2, output2) <- postRequest(
          uri.withPath(Uri.Path("/v1/user/rights")),
          domain.ListUserRightsRequest(user.id).toJson,
          headers = headersWithAdminAuth,
        )
        assertion <- {
          status2 shouldBe StatusCodes.OK
          assertStatus(output2, StatusCodes.OK)
          getResult(output2).convertTo[List[domain.UserRight]] should contain theSameElementsAs
            List[domain.UserRight](
              domain.CanActAs(alice)
            )
        }
      } yield assertion
  }

  "creating and listing 20K users should be possible" in withHttpServiceAndClient {
    (uri, _, _, _, _) =>
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
        (status, output) <- getRequest(
          uri.withPath(Uri.Path("/v1/users")),
          headers = headersWithAdminAuth,
        )
      } yield {
        status shouldBe StatusCodes.OK
        val userIds = getResult(output).convertTo[List[UserDetails]].map(_.userId)
        val expectedUserIds = "participant_admin" :: createUserRequests.map(_.userId)
        userIds should contain allElementsOf expectedUserIds
      }
  }
}

class HttpServiceIntegrationTestUserManagement
    extends HttpServiceIntegrationTestUserManagementNoAuth
    with SandboxRequiringAuthorization
