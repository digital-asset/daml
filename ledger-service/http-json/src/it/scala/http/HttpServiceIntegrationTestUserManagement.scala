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
import org.scalatest.{AsyncTestSuite, Inside}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList
import scalaz.syntax.show._
import spray.json.JsValue

import scala.concurrent.Future
import scalaz.syntax.tag._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
trait HttpServiceIntegrationTestUserManagementFuns
    extends AbstractHttpServiceIntegrationTestFuns
    with SandboxRequiringAuthorizationFuns {
  this: AsyncTestSuite with Matchers with Inside =>

  def jwtForUser(userId: String): Jwt =
    Jwt(toHeader(standardToken(userId)))

  val participantAdminJwt: Jwt = Jwt(toHeader(adminTokenStandardJWT))

  def createUser(ledgerClient: DamlLedgerClient)(
      userId: Ref.UserId,
      primaryParty: Option[Ref.Party] = None,
      initialRights: List[UserRight] = List.empty,
  ): Future[User] =
    ledgerClient.userManagementClient.createUser(
      User(userId, primaryParty),
      initialRights,
      Some(participantAdminJwt.value),
    )

  def headersWithUserAuth(userId: String): List[Authorization] =
    authorizationHeader(jwtForUser(userId))

  def getUniqueUserName(name: String): String = getUniqueParty(name).unwrap

}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class HttpServiceIntegrationTestUserManagementNoAuth
    extends AsyncFreeSpec
    with Matchers
    with Inside
    with StrictLogging
    with HttpServiceIntegrationTestUserManagementFuns {

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

  "create IOU should work with correct user rights" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, encoder, _, ledgerClient, _) =>
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

  "create IOU should fail if user has no permission" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, encoder, _, ledgerClient, _) =>
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

  "create IOU should fail if overwritten actAs & readAs result in missing permission even if the user would have the rights" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, encoder, _, ledgerClient, _) =>
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

  "requesting the user id should be possible via the user endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, ledgerClient, _) =>
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

  "requesting the user rights for a specific user should be possible via a POST to the user/rights endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, ledgerClient, _) =>
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
        headers = authorizationHeader(participantAdminJwt),
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

  "requesting the user rights for the current user should be possible via a GET to the user/rights endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, ledgerClient, _) =>
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

  "creating a user should be possible via the user/create endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, _, _) =>
    import spray.json._
    import spray.json.DefaultJsonProtocol._
    val alice = getUniqueParty("Alice")
    val createUserRequest = domain.CreateUserRequest(
      "nice.user2",
      Some(alice.unwrap),
      List[domain.UserRight](
        domain.CanActAs(alice),
        domain.ParticipantAdmin,
      ),
    )
    for {
      (status, output) <- postRequest(
        uri.withPath(Uri.Path("/v1/user/create")),
        createUserRequest.toJson,
        headers = authorizationHeader(participantAdminJwt),
      )
      assertion <- {
        status shouldBe StatusCodes.OK
        getResult(output).convertTo[Boolean] shouldBe true
      }
    } yield assertion
  }

  "getting all users should be possible via the users endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, _, _) =>
    import spray.json._
    import scalaz.std.scalaFuture._
    import scalaz.syntax.traverse._
    import scalaz.std.list._
    val alice = getUniqueParty("Alice")
    val usernames = List("nice.username", "nice.username2", "nice.username3").map(getUniqueUserName)
    val createUserRequests = usernames.map(name =>
      domain.CreateUserRequest(
        name,
        Some(alice.unwrap),
        List[domain.UserRight](
          domain.CanActAs(alice),
          domain.ParticipantAdmin,
        ),
      )
    )
    for {
      _ <- createUserRequests.traverse(createUserRequest =>
        for {
          (status, _) <- postRequest(
            uri.withPath(Uri.Path("/v1/user/create")),
            createUserRequest.toJson,
            headers = authorizationHeader(participantAdminJwt),
          )
          _ = status shouldBe StatusCodes.OK
        } yield ()
      )
      (status, output) <- getRequest(
        uri.withPath(Uri.Path("/v1/users")),
        headers = authorizationHeader(participantAdminJwt),
      )
      _ = status shouldBe StatusCodes.OK
      users = getResult(output).convertTo[List[UserDetails]]
    } yield users.map(_.userId) should contain allElementsOf usernames
  }

  "getting information about a specific user should be possible via the user endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, _, _) =>
    import spray.json._
    import spray.json.DefaultJsonProtocol._
    val alice = getUniqueParty("Alice")
    val createUserRequest = domain.CreateUserRequest(
      getUniqueUserName("nice.user"),
      Some(alice.unwrap),
      List[domain.UserRight](
        domain.CanActAs(alice),
        domain.ParticipantAdmin,
      ),
    )
    for {
      (status1, output1) <- postRequest(
        uri.withPath(Uri.Path("/v1/user/create")),
        createUserRequest.toJson,
        headers = authorizationHeader(participantAdminJwt),
      )
      _ <- {
        status1 shouldBe StatusCodes.OK
        getResult(output1).convertTo[Boolean] shouldBe true
      }
      (status2, output2) <- postRequest(
        uri.withPath(Uri.Path(s"/v1/user")),
        domain.GetUserRequest(createUserRequest.userId).toJson,
        headers = authorizationHeader(participantAdminJwt),
      )
    } yield {
      status2 shouldBe StatusCodes.OK
      getResult(output2).convertTo[UserDetails] shouldBe UserDetails(
        createUserRequest.userId,
        createUserRequest.primaryParty,
      )
    }
  }

  "getting information about the current user should be possible via the user endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, _, _) =>
    import spray.json._
    import spray.json.DefaultJsonProtocol._
    val alice = getUniqueParty("Alice")
    val createUserRequest = domain.CreateUserRequest(
      getUniqueUserName("nice.user"),
      Some(alice.unwrap),
      List[domain.UserRight](
        domain.CanActAs(alice),
        domain.ParticipantAdmin,
      ),
    )
    for {
      (status1, output1) <- postRequest(
        uri.withPath(Uri.Path("/v1/user/create")),
        createUserRequest.toJson,
        headers = authorizationHeader(participantAdminJwt),
      )
      _ <- {
        status1 shouldBe StatusCodes.OK
        getResult(output1).convertTo[Boolean] shouldBe true
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

  "deleting a specific user should be possible via the user/delete endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, _, _) =>
    import spray.json._
    import spray.json.DefaultJsonProtocol._
    val alice = getUniqueParty("Alice")
    val createUserRequest = domain.CreateUserRequest(
      getUniqueUserName("nice.user"),
      Some(alice.unwrap),
      List[domain.UserRight](
        domain.CanActAs(alice),
        domain.ParticipantAdmin,
      ),
    )
    for {
      (status1, output1) <- postRequest(
        uri.withPath(Uri.Path("/v1/user/create")),
        createUserRequest.toJson,
        headers = authorizationHeader(participantAdminJwt),
      )
      _ <- {
        status1 shouldBe StatusCodes.OK
        getResult(output1).convertTo[Boolean] shouldBe true
      }
      (status2, _) <- postRequest(
        uri.withPath(Uri.Path(s"/v1/user/delete")),
        domain.DeleteUserRequest(createUserRequest.userId).toJson,
        headers = authorizationHeader(participantAdminJwt),
      )
      _ = status2 shouldBe StatusCodes.OK
      (status3, output3) <- getRequest(
        uri.withPath(Uri.Path("/v1/users")),
        headers = authorizationHeader(participantAdminJwt),
      )
    } yield {
      status3 shouldBe StatusCodes.OK
      getResult(output3).convertTo[List[UserDetails]] should not contain createUserRequest.userId
    }
  }

  "granting the user rights for a specific user should be possible via a POST to the user/rights/grant endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, ledgerClient, _) =>
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
        headers = authorizationHeader(participantAdminJwt),
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
        headers = authorizationHeader(participantAdminJwt),
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

  "revoking the user rights for a specific user should be possible via a POST to the user/rights/revoke endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, ledgerClient, _) =>
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
        headers = authorizationHeader(participantAdminJwt),
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
        headers = authorizationHeader(participantAdminJwt),
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
}

class HttpServiceIntegrationTestUserManagement
    extends HttpServiceIntegrationTestUserManagementNoAuth
    with SandboxRequiringAuthorization
