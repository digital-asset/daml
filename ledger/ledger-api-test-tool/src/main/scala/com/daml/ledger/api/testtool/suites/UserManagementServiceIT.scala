// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  GetUserRequest,
  GrantUserRightsRequest,
  ListUserRightsRequest,
  RevokeUserRightsRequest,
  User,
  Right => Permission,
}
import com.daml.ledger.api.v1.admin.{user_management_service => proto}
import io.grpc.Status

import scala.concurrent.Future

final class UserManagementServiceIT extends LedgerTestSuite {
  // TODO (i12059): complete testing
  // create user
  // - invalid user-name
  // - invalid rights

  // get user
  // delete user
  // list users
  // grant user rights
  // revoke user rights
  // list user rights
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
        errorCode: ErrorCode,
    ): Future[Unit] =
      for {
        error <- ledger.userManagement
          .createUser(CreateUserRequest(Some(user), rights))
          .mustFail(problem)
      } yield assertGrpcError(ledger, error, Status.Code.INVALID_ARGUMENT, errorCode, None)

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
    def getAndCheck(problem: String, userId: String, errorCode: ErrorCode): Future[Unit] =
      for {
        error <- ledger.userManagement
          .getUser(GetUserRequest(userId))
          .mustFail(problem)
      } yield assertGrpcError(ledger, error, Status.Code.INVALID_ARGUMENT, errorCode, None)

    for {
      _ <- getAndCheck("empty user-id", "", LedgerApiErrors.RequestValidation.InvalidArgument)
      _ <- getAndCheck("invalid user-id", "!!", LedgerApiErrors.RequestValidation.InvalidField)
    } yield ()
  })

  test(
    "TestAllUserManagementRpcs",
    "Exercise every rpc once with success and once with a failure",
    allocate(NoParties),
    enabled = _.userManagement,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger)) =>
    for {
      // TODO: actually exercise all RPCs
      createResult <- ledger.userManagement.createUser(CreateUserRequest(Some(User("a", "b")), Nil))
      createAgainError <- ledger.userManagement
        .createUser(CreateUserRequest(Some(User("a", "b")), Nil))
        .mustFail("allocating a duplicate user")

      getUserResult <- ledger.userManagement.getUser(GetUserRequest("a"))
      getUserError <- ledger.userManagement
        .getUser(GetUserRequest("b"))
        .mustFail("retrieving non-existent user")

      grantResult <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(
          "a",
          List(Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))),
        )
      )
      listRightsResult <- ledger.userManagement.listUserRights(ListUserRightsRequest("a"))
      revokeResult <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(
          "a",
          List(Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))),
        )
      )
      _ <- ledger.userManagement.deleteUser(DeleteUserRequest("a"))
    } yield {
      assertGrpcError(
        ledger,
        createAgainError,
        Status.Code.ALREADY_EXISTS,
        LedgerApiErrors.AdminServices.UserAlreadyExists,
        None,
      )
      assertGrpcError(
        ledger,
        getUserError,
        Status.Code.NOT_FOUND,
        LedgerApiErrors.AdminServices.UserNotFound,
        None,
      )
      assert(createResult == User("a", "b"))
      assert(getUserResult == User("a", "b"))
      assert(
        grantResult.newlyGrantedRights == List(
          Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))
        )
      )
      assert(
        revokeResult.newlyRevokedRights == List(
          Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))
        )
      )
      assert(
        listRightsResult.rights.toSet == Set(
          Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))
//          Permission(Permission.Kind.CanActAs(Permission.CanActAs("acting-party"))),
//          Permission(Permission.Kind.CanReadAs(Permission.CanReadAs("reader-party"))),
        )
      )
    }
  })
}
