// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import java.util.UUID

import com.daml.error.ErrorCode
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
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
import scala.concurrent.{ExecutionContext, Future}

final class UserManagementServiceIT extends LedgerTestSuite {

  private val adminPermission =
    Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))
  private val actAsPermission1 =
    Permission(Permission.Kind.CanActAs(Permission.CanActAs("acting-party-1")))
  private val readAsPermission1 =
    Permission(Permission.Kind.CanReadAs(Permission.CanReadAs("reading-party-1")))
  private val userRightsBatch = List(
    actAsPermission1,
    Permission(Permission.Kind.CanActAs(Permission.CanActAs("acting-party-2"))),
    readAsPermission1,
    Permission(Permission.Kind.CanReadAs(Permission.CanReadAs("reading-party-2"))),
  )
  private val AdminUserId = "participant_admin"

  test(
    "UserManagementUserRightsLimit",
    "Test user rights per user limit",
    allocate(NoParties),
    enabled = _.userManagement.maxRightsPerUser > 0,
    disabledReason = "requires user management feature with user rights limit",
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

    def createCanActAs(id: Int) =
      Permission(Permission.Kind.CanActAs(Permission.CanActAs(s"acting-party-$id")))

    val user1 = User(UUID.randomUUID.toString, "")
    val user2 = User(UUID.randomUUID.toString, "")

    val maxRightsPerUser = ledger.features.userManagement.maxRightsPerUser
    val permissionsMaxAndOne = (1 to (maxRightsPerUser + 1)).map(createCanActAs)
    val permissionOne = permissionsMaxAndOne.head
    val permissionsMax = permissionsMaxAndOne.tail

    for {
      // cannot create user with #limit+1 rights
      create1 <- ledger.userManagement
        .createUser(CreateUserRequest(Some(user1), permissionsMaxAndOne))
        .mustFail(
          "creating user with too many rights"
        )
      // can create user with #limit rights
      create2 <- ledger.userManagement.createUser(CreateUserRequest(Some(user1), permissionsMax))
      // fails adding one more right
      grant1 <- ledger.userManagement
        .grantUserRights(GrantUserRightsRequest(user1.id, rights = Seq(permissionOne)))
        .mustFail(
          "granting more rights exceeds max number of user rights per user"
        )
      // rights already added are intact
      rights1 <- ledger.userManagement.listUserRights(ListUserRightsRequest(user1.id))
      // can create other users with #limit rights
      create3 <- ledger.userManagement.createUser(CreateUserRequest(Some(user2), permissionsMax))
      // cleanup
      _ <- ledger.userManagement.deleteUser(DeleteUserRequest(user1.id))
      _ <- ledger.userManagement.deleteUser(DeleteUserRequest(user2.id))

    } yield {
      assertTooManyUserRightsError(create1)
      assertEquals(create2, user1)
      assertTooManyUserRightsError(grant1)
      assertEquals(rights1.rights.size, permissionsMaxAndOne.tail.size)
      assertSameElements(rights1.rights, permissionsMaxAndOne.tail)
      assertEquals(create3, user2)
    }
  })

  test(
    "UserManagementCreateUserInvalidArguments",
    "Test argument validation for UserManagement#CreateUser",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val userId = UUID.randomUUID.toString

    def createAndCheck(
        problem: String,
        user: User,
        rights: Seq[proto.Right],
        expectedErrorCode: ErrorCode,
    ): Future[Unit] = {
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
    }

    for {
      _ <- createAndCheck(
        "empty user-id",
        User(""),
        List.empty,
        LedgerApiErrors.RequestValidation.MissingField,
      )
      _ <- createAndCheck(
        "invalid user-id",
        User("?"),
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
    enabled = _.userManagement.supported,
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
      _ <- getAndCheck("invalid user-id", "?", LedgerApiErrors.RequestValidation.InvalidField)
    } yield ()
  })

  userManagementTest(
    "TestAdminExists",
    "Ensure admin user exists",
  )(implicit ec => { implicit ledger =>
    for {
      get1 <- ledger.userManagement.getUser(GetUserRequest(AdminUserId))
      rights1 <- ledger.userManagement.listUserRights(ListUserRightsRequest(AdminUserId))
    } yield {
      assertEquals(get1, User(AdminUserId, ""))
      assertEquals(rights1, ListUserRightsResponse(Seq(adminPermission)))
    }
  })

  userManagementTest(
    "TestCreateUser",
    "Exercise CreateUser rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(userId1, "party1")
    val user2 = User(userId2, "")
    for {
      res1 <- ledger.userManagement.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res2 <- ledger.userManagement
        .createUser(CreateUserRequest(Some(user1), Nil))
        .mustFail("allocating a duplicate user")
      res3 <- ledger.userManagement.createUser(CreateUserRequest(Some(user2), Nil))
      res4 <- ledger.userManagement.deleteUser(DeleteUserRequest(userId2))
    } yield {
      assertEquals(res1, user1)
      assertUserAlreadyExists(res2)
      assertEquals(res3, user2)
      assertEquals(res4, DeleteUserResponse())
    }
  })

  userManagementTest(
    "TestGetUser",
    "Exercise GetUser rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(userId1, "party1")
    for {
      _ <- ledger.userManagement.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res1 <- ledger.userManagement.getUser(GetUserRequest(userId1))
      res2 <- ledger.userManagement
        .getUser(GetUserRequest(userId2))
        .mustFail("retrieving non-existent user")
    } yield {
      assertUserNotFound(res2)
      assert(res1 == user1)
    }
  })

  userManagementTest(
    "TestDeleteUser",
    "Exercise DeleteUser rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(userId1, "party1")
    for {
      _ <- ledger.userManagement.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res1 <- ledger.userManagement.deleteUser(DeleteUserRequest(userId1))
      res2 <- ledger.userManagement
        .deleteUser(DeleteUserRequest(userId2))
        .mustFail("deleting non-existent user")
    } yield {
      assertEquals(res1, DeleteUserResponse())
      assertUserNotFound(res2)
    }
  })

  userManagementTest(
    "TestListUsers",
    "Exercise ListUsers rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(userId1, "party1")
    val user2 = User(userId2, "party4")
    for {
      _ <- ledger.userManagement.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res1 <- ledger.userManagement.listUsers(ListUsersRequest())
      res2 <- ledger.userManagement.createUser(
        CreateUserRequest(Some(user2), Nil)
      )
      res3 <- ledger.userManagement.listUsers(ListUsersRequest())
      res4 <- ledger.userManagement.deleteUser(DeleteUserRequest(userId2))
      res5 <- ledger.userManagement.listUsers(ListUsersRequest())
    } yield {
      def filterUsers(users: Iterable[User]) = users.filter(u => u.id == userId1 || u.id == userId2)

      assertSameElements(filterUsers(res1.users), Seq(user1))
      assertEquals(res2, user2)
      assertSameElements(
        filterUsers(res3.users),
        Set(user1, user2),
      )
      assertEquals(res4, DeleteUserResponse())
      assertSameElements(filterUsers(res5.users), Seq(user1))
    }
  })

  userManagementTest(
    "TestGrantUserRights",
    "Exercise GrantUserRights rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(userId1, "party1")
    for {
      _ <- ledger.userManagement.createUser(CreateUserRequest(Some(user1), Nil))
      res1 <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(userId1, List(adminPermission))
      )
      res2 <- ledger.userManagement
        .grantUserRights(GrantUserRightsRequest(userId2, List(adminPermission)))
        .mustFail("granting right to a non-existent user")
      res3 <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(userId1, List(adminPermission))
      )
      res4 <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(userId1, userRightsBatch)
      )
    } yield {
      assertSameElements(res1.newlyGrantedRights, List(adminPermission))
      assertUserNotFound(res2)
      assertSameElements(res3.newlyGrantedRights, List.empty)
      assertSameElements(res4.newlyGrantedRights.toSet, userRightsBatch.toSet)
    }
  })

  userManagementTest(
    "TestRevokeUserRights",
    "Exercise RevokeUserRights rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(userId1, "party1")
    for {
      _ <- ledger.userManagement.createUser(
        CreateUserRequest(Some(user1), List(adminPermission) ++ userRightsBatch)
      )
      res1 <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(userId1, List(adminPermission))
      )
      res2 <- ledger.userManagement
        .revokeUserRights(RevokeUserRightsRequest(userId2, List(adminPermission)))
        .mustFail("revoking right from a non-existent user")
      res3 <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(userId1, List(adminPermission))
      )
      res4 <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(userId1, userRightsBatch)
      )
    } yield {
      assertEquals(res1, RevokeUserRightsResponse(List(adminPermission)))
      assertUserNotFound(res2)
      assertSameElements(res3.newlyRevokedRights, List.empty)
      assertSameElements(res4.newlyRevokedRights.toSet, userRightsBatch.toSet)
    }
  })

  userManagementTest(
    "TestListUserRights",
    "Exercise ListUserRights rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val user1 = User(userId1, "party4")
    for {
      res1 <- ledger.userManagement.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res2 <- ledger.userManagement.listUserRights(ListUserRightsRequest(userId1))
      res3 <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(
          userId1,
          List(adminPermission, actAsPermission1, readAsPermission1),
        )
      )
      res4 <- ledger.userManagement.listUserRights(ListUserRightsRequest(userId1))
      res5 <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(userId1, List(adminPermission))
      )
      res6 <- ledger.userManagement
        .listUserRights(ListUserRightsRequest(userId1))
    } yield {
      assertEquals(res1, user1)
      assertEquals(res2, ListUserRightsResponse(Seq.empty))
      assertSameElements(
        res3.newlyGrantedRights.toSet,
        Set(adminPermission, actAsPermission1, readAsPermission1),
      )
      assertSameElements(
        res4.rights.toSet,
        Set(adminPermission, actAsPermission1, readAsPermission1),
      )
      assertSameElements(res5.newlyRevokedRights, Seq(adminPermission))
      assertSameElements(res6.rights.toSet, Set(actAsPermission1, readAsPermission1))
    }
  })

  private def userManagementTest(
      shortIdentifier: String,
      description: String,
  )(
      body: ExecutionContext => ParticipantTestContext => Future[Unit]
  ): Unit = {
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      allocate(NoParties),
      enabled = _.userManagement.supported,
      disabledReason = "requires user management feature",
    )(implicit ec => { case Participants(Participant(ledger)) =>
      body(ec)(ledger)
    })
  }

  private def assertUserNotFound(t: Throwable)(implicit ledger: ParticipantTestContext): Unit = {
    assertGrpcError(
      participant = ledger,
      t = t,
      expectedCode = Status.Code.NOT_FOUND,
      selfServiceErrorCode = LedgerApiErrors.AdminServices.UserNotFound,
      exceptionMessageSubstring = None,
    )
  }

  private def assertUserAlreadyExists(
      t: Throwable
  )(implicit ledger: ParticipantTestContext): Unit = {
    assertGrpcError(
      participant = ledger,
      t = t,
      expectedCode = Status.Code.ALREADY_EXISTS,
      selfServiceErrorCode = LedgerApiErrors.AdminServices.UserAlreadyExists,
      exceptionMessageSubstring = None,
    )
  }

}
