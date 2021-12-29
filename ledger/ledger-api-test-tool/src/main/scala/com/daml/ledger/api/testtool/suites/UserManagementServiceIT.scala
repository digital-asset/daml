// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.util.UUID

import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  DeleteUserRequest,
  DeleteUserResponse,
  GetUserRequest,
  GrantUserRightsRequest,
  ListUserRightsRequest,
  ListUserRightsResponse,
  ListUsersRequest,
  RevokeUserRightsRequest,
  RevokeUserRightsResponse,
  User,
  Right => Permission,
}
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import io.grpc.Status

import scala.collection.immutable.Iterable
import scala.concurrent.Future
import scala.util.Random

// TODO participant user management: Test what error message is served when an operation are done on an non-existent user.
final class UserManagementServiceIT extends LedgerTestSuite {

  def assertSameElements[T](actual: Iterable[T], expected: Iterable[T]): Unit = {
    assert(
      actual.toSet == expected.toSet,
      s"Actual ${actual.mkString(", ")} should have the same elements as (expected): ${expected.mkString(", ")}",
    )
  }

  def assertEquals(actual: Any, expected: Any): Unit = {
    assert(actual == expected, s"Actual |${actual}| should be equal (expected): |${expected}|")
  }

  test(
    "UserManagementUserRightsLimit",
    "Test 1000 user rights per user limit",
    allocate(NoParties),
    enabled = _.userManagement,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def assertTooManyUserRightsError(t: Throwable): Unit = {
      assertGrpcError(
        participant = ledger,
        t = t,
        expectedCode = Status.Code.FAILED_PRECONDITION,
        selfServiceErrorCode = LedgerApiErrors.AdminServices.TooManyUserRights,
        exceptionMessageSubstring = None,
      )
    }

    val adminPermission =
      Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))

    def createCanActAs(id: Int) =
      Permission(Permission.Kind.CanActAs(Permission.CanActAs(s"acting-party-$id")))

    def createReadActAs(id: Int) =
      Permission(Permission.Kind.CanReadAs(Permission.CanReadAs(s"reading-party-$id")))

    val permissions1001: Seq[Permission] = Random.shuffle(
      (1 to 500).map(createCanActAs) ++ (1 to 500).map(createReadActAs) ++ Seq(adminPermission)
    )
    // TODO participant user management: Hardcoded: max number of user rights: 1000
    assertEquals(permissions1001.length, 1001)
    val permission1 = permissions1001.head
    val permissions1000 = permissions1001.tail

    val user1 = User(UUID.randomUUID.toString, "")
    val user2 = User(UUID.randomUUID.toString, "")

    for {
      // cannot create user with 1001 rights
      create1 <- ledger.userManagement
        .createUser(CreateUserRequest(Some(user1), permissions1001))
        .mustFail(
          "creating user with too many rights"
        )
      // can create user with 1000 rights
      create2 <- ledger.userManagement.createUser(CreateUserRequest(Some(user1), permissions1000))
      // fails adding one more right
      grant1 <- ledger.userManagement
        .grantUserRights(GrantUserRightsRequest(user1.id, rights = Seq(permission1)))
        .mustFail(
          "granting more rights exceeds max number of user rights per user"
        )
      // rights already added are intact
      rights1 <- ledger.userManagement.listUserRights(ListUserRightsRequest(user1.id))
      // can create other users with 1000 rights
      create3 <- ledger.userManagement.createUser(CreateUserRequest(Some(user2), permissions1000))

    } yield {
      assertTooManyUserRightsError(create1)
      assertEquals(create2, user1)
      assertTooManyUserRightsError(grant1)
      assertEquals(rights1.rights.size, permissions1001.tail.size)
      assertSameElements(rights1.rights, permissions1001.tail)
      assertEquals(create3, user2)
    }

  })

  test(
    "UserManagementCreateUserInvalidArguments",
    "Test argument validation for UserManagement#CreateUser",
    allocate(NoParties),
    enabled = _.userManagement,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val userId = UUID.randomUUID.toString

    def createAndCheck(
        problem: String,
        user: User,
        rights: Seq[proto.Right],
        expectedErrorCode: ErrorCode,
    ): Future[Unit] =
      for {
        throwable <- ledger.userManagement
          .createUser(CreateUserRequest(Some(user), rights))
          .mustFail(context = problem)
      } yield assertGrpcError(
        participant = ledger,
        t = throwable,
        expectedCode = Status.Code.INVALID_ARGUMENT,
        selfServiceErrorCode = expectedErrorCode,
        exceptionMessageSubstring = None,
      )

    for {
      _ <- createAndCheck(
        "empty user-id",
        User(""),
        List.empty,
        LedgerApiErrors.RequestValidation.InvalidField,
      )
      _ <- createAndCheck(
        "invalid user-id",
        User("!!"),
        List.empty,
        LedgerApiErrors.RequestValidation.InvalidField,
      )
      _ <- createAndCheck(
        "invalid primary-party",
        User("u1-" + userId, "party2-!!"),
        List.empty,
        LedgerApiErrors.RequestValidation.InvalidArgument,
      )
      r = proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs("party3-!!")))
      _ <- createAndCheck(
        "invalid party in right",
        User("u2-" + userId),
        List(r),
        LedgerApiErrors.RequestValidation.InvalidArgument,
      )
    } yield ()
  })

  test(
    "UserManagementGetUserInvalidArguments",
    "Test argument validation for UserManagement#GetUser",
    allocate(NoParties),
    enabled = _.userManagement,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    def getAndCheck(problem: String, userId: String, expectedErrorCode: ErrorCode): Future[Unit] =
      for {
        error <- ledger.userManagement
          .getUser(GetUserRequest(userId))
          .mustFail(problem)
      } yield assertGrpcError(ledger, error, Status.Code.INVALID_ARGUMENT, expectedErrorCode, None)

    for {
      _ <- getAndCheck("empty user-id", "", LedgerApiErrors.RequestValidation.InvalidArgument)
      _ <- getAndCheck("invalid user-id", "!!", LedgerApiErrors.RequestValidation.InvalidField)
    } yield ()
  })

  test(
    "TestAllUserManagementRpcs",
    "Exercise every rpc at least once with success and at least once with a failure",
    allocate(NoParties),
    enabled = _.userManagement,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val adminPermission =
      Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))
    val actAsPermission1 =
      Permission(Permission.Kind.CanActAs(Permission.CanActAs("acting-party-1")))
    val readAsPermission1 =
      Permission(Permission.Kind.CanReadAs(Permission.CanReadAs("reading-party-1")))

    val userRightsBatch = List(
      actAsPermission1,
      Permission(Permission.Kind.CanActAs(Permission.CanActAs("acting-party-2"))),
      readAsPermission1,
      Permission(Permission.Kind.CanReadAs(Permission.CanReadAs("reading-party-2"))),
    )

    def assertUserNotFound(t: Throwable): Unit = {
      assertGrpcError(
        participant = ledger,
        t = t,
        expectedCode = Status.Code.NOT_FOUND,
        selfServiceErrorCode = LedgerApiErrors.AdminServices.UserNotFound,
        exceptionMessageSubstring = None,
      )
    }

    def assertUserAlreadyExists(t: Throwable): Unit = {
      assertGrpcError(
        participant = ledger,
        t = t,
        expectedCode = Status.Code.ALREADY_EXISTS,
        selfServiceErrorCode = LedgerApiErrors.AdminServices.UserAlreadyExists,
        exceptionMessageSubstring = None,
      )
    }

    def testCreateUser(): Future[Unit] = {
      for {
        res1 <- ledger.userManagement.createUser(
          CreateUserRequest(Some(User("user1", "party1")), Nil)
        )
        res2 <- ledger.userManagement
          .createUser(CreateUserRequest(Some(User("user1", "party1")), Nil))
          .mustFail("allocating a duplicate user")
        res3 <- ledger.userManagement.createUser(CreateUserRequest(Some(User("user3", "")), Nil))
        res4 <- ledger.userManagement.deleteUser(DeleteUserRequest("user3"))
      } yield {
        assertEquals(res1, User("user1", "party1"))
        assertUserAlreadyExists(res2)
        assertEquals(res3, User("user3", ""))
        assertEquals(res4, DeleteUserResponse())
      }
    }

    def testGetUser(): Future[Unit] = {
      for {
        res1 <- ledger.userManagement.getUser(GetUserRequest("user1"))
        res2 <- ledger.userManagement
          .getUser(GetUserRequest("user2"))
          .mustFail("retrieving non-existent user")
      } yield {
        assertUserNotFound(res2)
        assert(res1 == User("user1", "party1"))
      }
    }

    def testDeleteUser(): Future[Unit] = {
      for {
        res1 <- ledger.userManagement.deleteUser(DeleteUserRequest("user1"))
        res2 <- ledger.userManagement
          .deleteUser(DeleteUserRequest("user2"))
          .mustFail("deleting non-existent user")
      } yield {
        assertEquals(res1, DeleteUserResponse())
        assertUserNotFound(res2)
      }
    }

    val adminUser = User("participant_admin", "")

    def testListUsers(): Future[Unit] = {
      for {
        res1 <- ledger.userManagement.listUsers(ListUsersRequest())
        res2 <- ledger.userManagement.createUser(
          CreateUserRequest(Some(User("user4", "party4")), Nil)
        )
        res3 <- ledger.userManagement.listUsers(ListUsersRequest())
        res4 <- ledger.userManagement.deleteUser(DeleteUserRequest("user4"))
        res5 <- ledger.userManagement.listUsers(ListUsersRequest())
      } yield {
        assertSameElements(res1.users, Seq(User("user1", "party1"), adminUser))
        assertEquals(res2, User("user4", "party4"))
        assertSameElements(
          res3.users,
          Set(User("user1", "party1"), User("user4", "party4"), adminUser),
        )
        assertEquals(res4, DeleteUserResponse())
        assertSameElements(res5.users, Seq(User("user1", "party1"), adminUser))
      }
    }

    def testGrantUserRights(): Future[Unit] = {

      for {
        res1 <- ledger.userManagement.grantUserRights(
          GrantUserRightsRequest("user1", List(adminPermission))
        )
        res2 <- ledger.userManagement
          .grantUserRights(GrantUserRightsRequest("user2", List(adminPermission)))
          .mustFail("granting right to a non-existent user")
        res3 <- ledger.userManagement.grantUserRights(
          GrantUserRightsRequest("user1", List(adminPermission))
        )
        res4 <- ledger.userManagement.grantUserRights(
          GrantUserRightsRequest("user1", userRightsBatch)
        )
      } yield {
        assertSameElements(res1.newlyGrantedRights, List(adminPermission))
        assertUserNotFound(res2)
        assertSameElements(res3.newlyGrantedRights, List.empty)
        assertSameElements(res4.newlyGrantedRights, userRightsBatch)
      }
    }

    def testRevokeUserRights(): Future[Unit] = {
      for {
        res1 <- ledger.userManagement.revokeUserRights(
          RevokeUserRightsRequest("user1", List(adminPermission))
        )
        res2 <- ledger.userManagement
          .revokeUserRights(RevokeUserRightsRequest("user2", List(adminPermission)))
          .mustFail("revoking right from a non-existent user")
        res3 <- ledger.userManagement.revokeUserRights(
          RevokeUserRightsRequest("user1", List(adminPermission))
        )
        res4 <- ledger.userManagement.revokeUserRights(
          RevokeUserRightsRequest("user1", userRightsBatch)
        )
      } yield {
        assertEquals(res1, RevokeUserRightsResponse(List(adminPermission)))
        assertUserNotFound(res2)
        assertSameElements(res3.newlyRevokedRights, List.empty)
        assertSameElements(res4.newlyRevokedRights, userRightsBatch)
      }
    }

    def testListUserRights(): Future[Unit] = {
      for {
        res1 <- ledger.userManagement.createUser(
          CreateUserRequest(Some(User("user4", "party4")), Nil)
        )
        res2 <- ledger.userManagement.listUserRights(ListUserRightsRequest("user4"))
        res3 <- ledger.userManagement.grantUserRights(
          GrantUserRightsRequest(
            "user4",
            List(adminPermission, actAsPermission1, readAsPermission1),
          )
        )
        res4 <- ledger.userManagement.listUserRights(ListUserRightsRequest("user4"))
        res5 <- ledger.userManagement.revokeUserRights(
          RevokeUserRightsRequest("user4", List(adminPermission))
        )
        res6 <- ledger.userManagement
          .listUserRights(ListUserRightsRequest("user4"))
      } yield {
        assertEquals(res1, User("user4", "party4"))
        assertEquals(res2, ListUserRightsResponse(Seq.empty))
        assertSameElements(
          res3.newlyGrantedRights,
          Set(adminPermission, actAsPermission1, readAsPermission1),
        )
        assertSameElements(
          res4.rights,
          Set(adminPermission, actAsPermission1, readAsPermission1),
        )
        assertSameElements(res5.newlyRevokedRights, Seq(adminPermission))
        assertSameElements(res6.rights, Set(actAsPermission1, readAsPermission1))
      }
    }

    for {
      _ <- testCreateUser()
      _ <- testGetUser()
      _ <- testListUsers()
      _ <- testGrantUserRights()
      _ <- testRevokeUserRights()
      _ <- testListUserRights()
      _ <- testDeleteUser()
    } yield {
      ()
    }
  })
}
