// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.http.scaladsl.model.headers.Authorization
import akka.http.scaladsl.model.{StatusCodes, Uri}
import com.daml.fetchcontracts.domain.TemplateId.OptionalPkg
import com.daml.http.HttpServiceTestFixture.{UseTls, authorizationHeader, getResult, postRequest}
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.http.dbbackend.JdbcConfig
import com.daml.http.domain.{UserDetails, UserRights}
import com.daml.http.json.JsonProtocol._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.auth.StandardJWTPayload
import com.daml.ledger.api.domain.{User, UserRight}
import com.daml.ledger.api.domain.UserRight.CanActAs
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

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
trait HttpServiceIntegrationTestUserManagementFuns
    extends AbstractHttpServiceIntegrationTestFuns
    with SandboxRequiringAuthorizationFuns {
  this: AsyncTestSuite with Matchers with Inside =>

  def jwtForUser(userId: String, admin: Boolean = false): Jwt =
    Jwt(toHeader(StandardJWTPayload(standardToken(userId).payload.copy(admin = admin))))

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

  def headersWithUserAuth(userId: String, admin: Boolean = false): List[Authorization] =
    authorizationHeader(jwtForUser(userId, admin))

  def getUniqueUserName(name: String): String = getUniqueParty(name).toString

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

  import scalaz.syntax.tag._

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
          CanActAs(Ref.Party.assertFromString(alice.toString))
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
          CanActAs(Ref.Party.assertFromString(bob.toString))
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
          CanActAs(Ref.Party.assertFromString(alice.toString)),
          CanActAs(Ref.Party.assertFromString(bob.toString)),
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
          user.primaryParty.map(_.toString),
        )
      }
    } yield assertion
  }

  "requesting the user rights should be possible via the user/rights endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, ledgerClient, _) =>
    val alice = getUniqueParty("Alice")
    val bob = getUniqueParty("Bob")
    for {
      user <- createUser(ledgerClient)(
        Ref.UserId.assertFromString(getUniqueUserName("nice.user")),
        initialRights = List(
          CanActAs(Ref.Party.assertFromString(alice.toString)),
          CanActAs(Ref.Party.assertFromString(bob.toString)),
        ),
      )
      (status, output) <- getRequest(
        uri.withPath(Uri.Path("/v1/user/rights")),
        headers = headersWithUserAuth(user.id),
      )
      assertion <- {
        status shouldBe StatusCodes.OK
        assertStatus(output, StatusCodes.OK)
        getResult(output).convertTo[UserRights] shouldEqual UserRights(
          canActAs = List(alice, bob),
          canReadAs = List.empty,
          isAdmin = false,
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
      List(alice),
      List.empty,
      isAdmin = true,
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
        List(alice),
        List.empty,
        isAdmin = true,
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
      List(alice),
      List.empty,
      isAdmin = true,
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
      List(alice),
      List.empty,
      isAdmin = true,
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
        headers = headersWithUserAuth(createUserRequest.userId, admin = true),
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
      List(alice),
      List.empty,
      isAdmin = true,
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

  "deleting the current user should be possible via the user/delete endpoint" in withHttpServiceAndClient(
    participantAdminJwt
  ) { (uri, _, _, _, _) =>
    import spray.json._
    import spray.json.DefaultJsonProtocol._
    val alice = getUniqueParty("Alice")
    val createUserRequest = domain.CreateUserRequest(
      getUniqueUserName("nice.user"),
      Some(alice.unwrap),
      List(alice),
      List.empty,
      isAdmin = true,
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
      (status2, _) <- getRequest(
        uri.withPath(Uri.Path(s"/v1/user/delete")),
        headers = headersWithUserAuth(createUserRequest.userId),
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
}

class HttpServiceIntegrationTestUserManagement
    extends HttpServiceIntegrationTestUserManagementNoAuth
    with SandboxRequiringAuthorization
