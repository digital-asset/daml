// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertEquals, *}
import com.daml.ledger.api.testtool.infrastructure.Party
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v2.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v2.admin.user_management_service as proto
import com.daml.ledger.api.v2.admin.user_management_service.{
  CreateUserRequest,
  CreateUserResponse,
  DeleteUserRequest,
  DeleteUserResponse,
  GetUserRequest,
  GetUserResponse,
  GrantUserRightsRequest,
  GrantUserRightsResponse,
  ListUserRightsRequest,
  ListUserRightsResponse,
  ListUsersRequest,
  ListUsersResponse,
  RevokeUserRightsRequest,
  RevokeUserRightsResponse,
  Right as Permission,
  UpdateUserIdentityProviderIdRequest,
  User,
}
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.base.error.utils.ErrorDetails.matches
import com.digitalasset.base.error.{BaseError, ErrorCode}
import com.digitalasset.canton.auth.AuthorizationChecksErrors
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.error.IndexErrors
import com.digitalasset.canton.ledger.error.groups.{AdminServiceErrors, RequestValidationErrors}
import io.grpc.{Status, StatusRuntimeException}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class UserManagementServiceIT extends UserManagementServiceITBase {

  private val adminPermission =
    Permission(Permission.Kind.ParticipantAdmin(Permission.ParticipantAdmin()))
  private def actAsPermission(party: String) =
    Permission(Permission.Kind.CanActAs(Permission.CanActAs(party)))
  private def readAsPermission(party: String) =
    Permission(Permission.Kind.CanReadAs(Permission.CanReadAs(party)))
  private def executeAsPermission(party: String) =
    Permission(Permission.Kind.CanExecuteAs(Permission.CanExecuteAs(party)))

  private val AdminUserId = "participant_admin"

  def matchesOneOf(t: Throwable, errorCodes: ErrorCode*): Boolean =
    errorCodes.exists(matches(t, _))

  def isInternalError(t: Throwable): Boolean = t match {
    case e: StatusRuntimeException => isInternalError(e)
    case _ => false
  }

  def isInternalError(e: StatusRuntimeException): Boolean =
    e.getStatus.getCode == Status.Code.INTERNAL && e.getStatus.getDescription.startsWith(
      BaseError.RedactedMessage.Prefix
    )

  test(
    "UserManagementUpdateUserIdpWithNonDefaultIdps",
    "Test reassigning user to a different idp using non default idps",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val userId1 = ledger.nextUserId()
    val idpId1 = ledger.nextIdentityProviderId()
    val idpId2 = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId1)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId2)
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userId1,
              primaryParty = "",
              identityProviderId = idpId1,
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.userManagement.updateUserIdentityProviderId(
        UpdateUserIdentityProviderIdRequest(
          userId1,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = idpId2,
        )
      )
      get1 <- ledger.userManagement.getUser(
        GetUserRequest(userId = userId1, identityProviderId = idpId2)
      )
      _ <- ledger.userManagement.updateUserIdentityProviderId(
        UpdateUserIdentityProviderIdRequest(
          userId1,
          sourceIdentityProviderId = idpId2,
          targetIdentityProviderId = idpId1,
        )
      )
      get2 <- ledger.userManagement.getUser(
        GetUserRequest(userId = userId1, identityProviderId = idpId1)
      )
      error <- ledger.userManagement
        .getUser(
          GetUserRequest(userId = userId1, identityProviderId = idpId2)
        )
        .mustFail("requesting with wrong idp")
      // cleanup
      _ <- ledger.userManagement.updateUserIdentityProviderId(
        UpdateUserIdentityProviderIdRequest(
          userId1,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = "",
        )
      )
    } yield {
      assertEquals(get1.user.value.identityProviderId, idpId2)
      assertEquals(get2.user.value.identityProviderId, idpId1)
      assert(
        error.getMessage.startsWith("PERMISSION_DENIED"),
        s"Actual message: ${error.getMessage}",
      )

    }
  })

  test(
    "UserManagementUpdateUserIdpWithDefaultIdp",
    "Test reassigning user to a different idp using the default idp",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val userId1 = ledger.nextUserId()
    val idpId = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId)
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userId1,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.userManagement.updateUserIdentityProviderId(
        UpdateUserIdentityProviderIdRequest(
          userId1,
          sourceIdentityProviderId = "",
          targetIdentityProviderId = idpId,
        )
      )
      get1 <- ledger.userManagement.getUser(
        GetUserRequest(userId = userId1, identityProviderId = idpId)
      )
      _ <- ledger.userManagement.updateUserIdentityProviderId(
        UpdateUserIdentityProviderIdRequest(
          userId1,
          sourceIdentityProviderId = idpId,
          targetIdentityProviderId = "",
        )
      )
      get2 <- ledger.userManagement.getUser(
        GetUserRequest(userId = userId1, identityProviderId = "")
      )
    } yield {
      assertEquals(get1.user.value.identityProviderId, idpId)
      assertEquals(get2.user.value.identityProviderId, "")
    }
  })

  test(
    "UserManagementUpdateUserIdpNonExistentIdps",
    "Test reassigning user to a different idp when source or target idp doesn't exist",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val userId = ledger.nextUserId()
    val idpNonExistent = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userId,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.userManagement
        .updateUserIdentityProviderId(
          UpdateUserIdentityProviderIdRequest(
            userId,
            sourceIdentityProviderId = idpNonExistent,
            targetIdentityProviderId = "",
          )
        )
        .mustFailWith(
          "non existent source idp",
          RequestValidationErrors.InvalidArgument,
        )
      _ <- ledger.userManagement
        .updateUserIdentityProviderId(
          UpdateUserIdentityProviderIdRequest(
            userId,
            sourceIdentityProviderId = "",
            targetIdentityProviderId = idpNonExistent,
          )
        )
        .mustFailWith(
          "non existent target idp",
          RequestValidationErrors.InvalidArgument,
        )
    } yield ()
  })

  test(
    "UserManagementUpdateUserIdpMismatchedSourceIdp",
    "Test reassigning user to a different idp using mismatched source idp",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val userIdDefault = ledger.nextUserId()
    val userIdNonDefault = ledger.nextUserId()
    val idpIdNonDefault = ledger.nextIdentityProviderId()
    val idpIdTarget = ledger.nextIdentityProviderId()
    val idpIdMismatched = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpIdMismatched)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpIdNonDefault)
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpIdTarget)
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userIdDefault,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userIdNonDefault,
              primaryParty = "",
              identityProviderId = idpIdNonDefault,
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      error1 <- ledger.userManagement
        .updateUserIdentityProviderId(
          UpdateUserIdentityProviderIdRequest(
            userIdDefault,
            sourceIdentityProviderId = idpIdMismatched,
            targetIdentityProviderId = idpIdTarget,
          )
        )
        .mustFail("mismatched source idp id")
      error2 <- ledger.userManagement
        .updateUserIdentityProviderId(
          UpdateUserIdentityProviderIdRequest(
            userIdNonDefault,
            sourceIdentityProviderId = idpIdMismatched,
            targetIdentityProviderId = idpIdTarget,
          )
        )
        .mustFail("mismatched source idp id")
      // cleanup
      _ <- ledger.userManagement.updateUserIdentityProviderId(
        UpdateUserIdentityProviderIdRequest(
          userIdNonDefault,
          sourceIdentityProviderId = idpIdNonDefault,
          targetIdentityProviderId = "",
        )
      )
    } yield {
      assert(
        error1.getMessage.startsWith("PERMISSION_DENIED"),
        s"Actual message: ${error1.getMessage}",
      )
      assert(
        error2.getMessage.startsWith("PERMISSION_DENIED"),
        s"Actual message: ${error2.getMessage}",
      )
    }
  })

  test(
    "UserManagementUpdateUserIdpSourceAndTargetIdpTheSame",
    "Test reassigning user to a different idp but source and target idps are the same",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val userIdDefault = ledger.nextUserId()
    val userIdNonDefault = ledger.nextUserId()
    val idpId1 = ledger.nextIdentityProviderId()
    for {
      _ <- ledger.createIdentityProviderConfig(identityProviderId = idpId1)
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userIdDefault,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userIdNonDefault,
              primaryParty = "",
              identityProviderId = idpId1,
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.userManagement.updateUserIdentityProviderId(
        UpdateUserIdentityProviderIdRequest(
          userIdDefault,
          sourceIdentityProviderId = "",
          targetIdentityProviderId = "",
        )
      )
      get1 <- ledger.userManagement.getUser(
        GetUserRequest(userId = userIdDefault, identityProviderId = "")
      )
      _ <- ledger.userManagement.updateUserIdentityProviderId(
        UpdateUserIdentityProviderIdRequest(
          userIdNonDefault,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = idpId1,
        )
      )
      get2 <- ledger.userManagement.getUser(
        GetUserRequest(userId = userIdNonDefault, identityProviderId = idpId1)
      )
      // cleanup
      _ <- ledger.userManagement.updateUserIdentityProviderId(
        UpdateUserIdentityProviderIdRequest(
          userIdNonDefault,
          sourceIdentityProviderId = idpId1,
          targetIdentityProviderId = "",
        )
      )
    } yield {
      assertEquals("default idp", get1.user.value.identityProviderId, "")
      assertEquals("non default idp", get2.user.value.identityProviderId, idpId1)
    }
  })

  test(
    "UserManagementUserRightsLimit",
    "Test user rights per user limit",
    allocate(NoParties),
    enabled = features =>
      features.userManagement.maxRightsPerUser > 0 && features.userManagement.maxRightsPerUser <= 100,
    disabledReason = "requires user management feature with user rights limit",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    def assertTooManyUserRightsError(t: Throwable): Unit =
      assertGrpcError(
        t = t,
        errorCode = AdminServiceErrors.UserManagement.TooManyUserRights,
        exceptionMessageSubstring = None,
      )

    def createCanActAs(party: Party) =
      Permission(Permission.Kind.CanActAs(Permission.CanActAs(party)))

    def allocateParty(id: Int) =
      ledger.allocateParty(Some(s"acting-party-$id"))

    val user1 = newUser(UUID.randomUUID.toString)
    val user2 = newUser(UUID.randomUUID.toString)

    val maxRightsPerUser = ledger.features.userManagement.maxRightsPerUser
    val allocatePartiesMaxAndOne = (1 to (maxRightsPerUser + 1)).map(allocateParty)

    for {
      // allocating parties before user is created
      allocatedParties <- Future.sequence(allocatePartiesMaxAndOne)
      permissionsMaxPlusOne = allocatedParties.map(createCanActAs)
      permissionOne = permissionsMaxPlusOne.headOption.value
      permissionsMax = permissionsMaxPlusOne.drop(1)
      // cannot create user with #limit+1 rights
      create1 <- ledger
        .createUser(CreateUserRequest(Some(user1), permissionsMaxPlusOne))
        .mustFail(
          "creating user with too many rights"
        )
      // can create user with #limit rights
      create2 <- ledger.createUser(CreateUserRequest(Some(user1), permissionsMax))
      // fails adding one more right
      grant1 <- ledger.userManagement
        .grantUserRights(GrantUserRightsRequest(user1.id, rights = Seq(permissionOne), ""))
        .mustFail(
          "granting more rights exceeds max number of user rights per user"
        )
      // rights already added are intact
      rights1 <- ledger.userManagement.listUserRights(ListUserRightsRequest(user1.id, ""))
      // can create other users with #limit rights
      create3 <- ledger.createUser(CreateUserRequest(Some(user2), permissionsMax))
    } yield {
      assertTooManyUserRightsError(create1)
      assertEquals(unsetResourceVersion(create2), CreateUserResponse(Some(user1)))
      assertTooManyUserRightsError(grant1)
      assertEquals(rights1.rights.size, permissionsMaxPlusOne.drop(1).size)
      assertSameElements(rights1.rights, permissionsMaxPlusOne.drop(1))
      assertEquals(unsetResourceVersion(create3), CreateUserResponse(Some(user2)))
    }
  })

  test(
    "UserManagementCreateUserInvalidArguments",
    "Test argument validation for UserManagement#CreateUser",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val userId = UUID.randomUUID.toString

    def createAndCheck(
        problem: String,
        user: User,
        rights: Seq[proto.Right],
        expectedErrorCode: ErrorCode,
    ): Future[Unit] =
      for {
        throwable <- ledger
          .createUser(CreateUserRequest(Some(user), rights))
          .mustFail(context = problem)
      } yield assertGrpcError(
        t = throwable,
        errorCode = expectedErrorCode,
        exceptionMessageSubstring = None,
      )

    for {
      _ <- createAndCheck(
        "empty user-id",
        User.defaultInstance,
        List.empty,
        RequestValidationErrors.MissingField,
      )
      _ <- createAndCheck(
        "invalid user-id",
        User(
          id = "?",
          primaryParty = "",
          identityProviderId = "",
          isDeactivated = false,
          metadata = None,
        ),
        List.empty,
        RequestValidationErrors.InvalidField,
      )
      _ <- createAndCheck(
        "invalid primary-party",
        User(
          id = "u1-" + userId,
          primaryParty = "party2-!!",
          identityProviderId = "",
          isDeactivated = false,
          metadata = None,
        ),
        List.empty,
        RequestValidationErrors.InvalidArgument,
      )
      r = proto.Right(proto.Right.Kind.CanActAs(proto.Right.CanActAs("party3-!!")))
      _ <- createAndCheck(
        "invalid party in right",
        User(
          id = "u2-" + userId,
          primaryParty = "",
          identityProviderId = "",
          isDeactivated = false,
          metadata = None,
        ),
        List(r),
        RequestValidationErrors.InvalidArgument,
      )
    } yield ()
  })

  test(
    "UserManagementGetUserInvalidArguments",
    "Test argument validation for UserManagement#GetUser",
    allocate(NoParties),
    enabled = _.userManagement.supported,
    disabledReason = "requires user management feature",
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    def getAndCheck(
        problem: String,
        userId: String,
        expectedErrorCode: ErrorCode,
        messageAssertion: String,
    ): Future[Unit] =
      for {
        error <- ledger.userManagement
          .getUser(GetUserRequest(userId, ""))
          .mustFail(problem)
      } yield assertGrpcError(error, expectedErrorCode, Some(messageAssertion))

    for {
      _ <- getAndCheck(
        "empty user-id",
        "",
        AuthorizationChecksErrors.InvalidToken,
        "INVALID_ARGUMENT: INVALID_TOKEN",
      )
      _ <- getAndCheck(
        "empty user-id",
        "",
        AuthorizationChecksErrors.InvalidToken,
        "The submitted request is missing a user-id: requests with an empty user-id are only supported if there is an authenticated user",
      )
      _ <- getAndCheck(
        "invalid user-id",
        "?",
        RequestValidationErrors.InvalidField,
        "INVALID_ARGUMENT: INVALID_FIELD",
      )
      _ <- getAndCheck(
        "invalid user-id",
        "?",
        RequestValidationErrors.InvalidField,
        "The submitted command has a field with invalid value: Invalid field user_id: User ID \"?\" does not match regex",
      )
    } yield ()
  })

  userManagementTest(
    "RaceConditionCreateUsers",
    "Tests scenario of multiple concurrent create-user calls for the same user",
    runConcurrently = false,
  ) { implicit ec => participant => _ =>
    val attempts = (1 to 10).toVector
    val userId = participant.nextUserId()
    val request =
      CreateUserRequest(
        Some(
          User(
            id = userId,
            primaryParty = "",
            identityProviderId = "",
            isDeactivated = false,
            metadata = None,
          )
        ),
        rights = Seq.empty,
      )
    for {
      results <- Future
        .traverse(attempts) { _ =>
          participant.createUser(request).transform(Success(_))
        }
    } yield {
      assertSingleton(
        "successful user creation",
        results.filter(_.isSuccess),
      ).discard
      val unexpectedErrors = results.collect {
        case Failure(t)
            if !matchesOneOf(
              t,
              IndexErrors.DatabaseErrors.SqlTransientError,
              AdminServiceErrors.UserManagement.UserAlreadyExists,
            )
              && !isInternalError(t) =>
          t
      }
      assertIsEmpty(unexpectedErrors)
    }
  }

  userManagementTest(
    "RaceConditionDeleteUsers",
    "Tests scenario of multiple concurrent delete-user calls for the same user",
    runConcurrently = false,
  ) { implicit ec => participant => _ =>
    val attempts = (1 to 10).toVector
    val userId = participant.nextUserId()
    val createUserRequest =
      CreateUserRequest(
        Some(
          User(
            id = userId,
            primaryParty = "",
            identityProviderId = "",
            isDeactivated = false,
            metadata = None,
          )
        ),
        rights = Seq.empty,
      )
    val deleteUserRequest = DeleteUserRequest(userId = userId, "")
    for {
      _ <- participant.createUser(createUserRequest)
      results <- Future
        .traverse(attempts) { _ =>
          participant.deleteUser(deleteUserRequest).transform(Success(_))
        }
    } yield {
      assertSingleton(
        "successful user deletion",
        results.filter(_.isSuccess),
      ).discard
      val unexpectedErrors = results
        .collect {
          case Failure(t)
              if !matchesOneOf(
                t,
                IndexErrors.DatabaseErrors.SqlTransientError,
                AdminServiceErrors.UserManagement.UserNotFound,
              ) && !isInternalError(t) =>
            t
        }
      assertIsEmpty(unexpectedErrors)
    }
  }

  userManagementTest(
    "RaceConditionGrantRights",
    "Tests scenario of multiple concurrent grant-right calls for the same user and the same rights",
    runConcurrently = false,
  ) { implicit ec => participant => _ =>
    val attempts = (1 to 10).toVector
    val userId = participant.nextUserId()
    val suffix = UUID.randomUUID().toString
    val createUserRequest =
      CreateUserRequest(
        Some(
          User(
            id = userId,
            primaryParty = "",
            identityProviderId = "",
            isDeactivated = false,
            metadata = None,
          )
        ),
        rights = Seq.empty,
      )
    for {
      parties <- allocateParties(participant, suffix)
      userRights = getUserRights(parties)
      grantRightsRequest = GrantUserRightsRequest(userId = userId, rights = userRights, "")
      _ <- participant.createUser(createUserRequest)
      results <- Future.traverse(attempts) { _ =>
        participant.userManagement.grantUserRights(grantRightsRequest).transform(Success(_))
      }
    } yield {
      val successes = results.collect {
        case Success(resp @ GrantUserRightsResponse(newlyGrantedRights))
            if newlyGrantedRights.nonEmpty =>
          resp
      }
      assertSingleton("Success response", successes).discard
      assertSameElements(successes.headOption.value.newlyGrantedRights, userRights)
      val unexpectedErrors = results
        .collect {
          case Failure(t)
              if
              // Transient db error caused by unique constraint violation on H2
              !ErrorDetails.matches(t, IndexErrors.DatabaseErrors.SqlTransientError)
                &&
                  // Internal error caused by unique constraint violation on Postgres
                  !isInternalError(t) =>
            t
        }
      assertIsEmpty(unexpectedErrors)
    }
  }

  userManagementTest(
    "RaceConditionRevokeRights",
    "Tests scenario of multiple concurrent revoke-right calls for the same user and the same rights",
    runConcurrently = false,
  ) { implicit ec => participant => _ =>
    val attempts = (1 to 10).toVector
    val userId = participant.nextUserId()
    val suffix = UUID.randomUUID().toString
    for {
      parties <- allocateParties(participant, suffix)
      userRights = getUserRights(parties)
      createUserRequest =
        CreateUserRequest(
          Some(
            User(
              id = userId,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          rights = userRights,
        )
      _ <- participant.createUser(createUserRequest)
      revokeRightsRequest = RevokeUserRightsRequest(
        userId = userId,
        rights = userRights,
        identityProviderId = "",
      )
      results <- Future.traverse(attempts) { _ =>
        participant.userManagement.revokeUserRights(revokeRightsRequest).transform(Success(_))
      }
    } yield {
      assertSingleton(
        "Non empty revoke-rights responses",
        results.collect {
          case Success(RevokeUserRightsResponse(actuallyRevoked)) if actuallyRevoked.nonEmpty =>
            actuallyRevoked
        },
      ).discard
      assertIsEmpty(results.filter(_.isFailure))
    }
  }

  userManagementTest(
    "TestAdminExists",
    "Ensure admin user exists",
  ) { implicit ec => implicit ledger => _ =>
    for {
      get1 <- ledger.userManagement.getUser(GetUserRequest(AdminUserId, ""))
      rights1 <- ledger.userManagement.listUserRights(ListUserRightsRequest(AdminUserId, ""))
    } yield {
      assertEquals(
        get1.user,
        Some(
          User(
            id = AdminUserId,
            metadata = Some(
              ObjectMeta(
                resourceVersion = get1.getUser.getMetadata.resourceVersion,
                annotations = Map.empty,
              )
            ),
            primaryParty = "",
            identityProviderId = "",
            isDeactivated = false,
          )
        ),
      )
      assertEquals(rights1, ListUserRightsResponse(Seq(adminPermission)))
    }
  }

  userManagementTest(
    "TestCreateUser",
    "Exercise CreateUser rpc",
  ) { implicit ec => implicit ledger => _ =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(
      id = userId1,
      primaryParty = "party1",
      metadata = Some(ObjectMeta.defaultInstance),
      identityProviderId = "",
      isDeactivated = false,
    )
    val user2 = User(
      id = userId2,
      primaryParty = "",
      metadata = Some(ObjectMeta.defaultInstance),
      identityProviderId = "",
      isDeactivated = false,
    )
    for {
      res1 <- ledger.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res2 <- ledger
        .createUser(CreateUserRequest(Some(user1), Nil))
        .mustFail("allocating a duplicate user")
      res3 <- ledger.createUser(CreateUserRequest(Some(user2), Nil))
      res4 <- ledger.deleteUser(DeleteUserRequest(userId2, ""))
    } yield {
      assertEquals(unsetResourceVersion(res1), CreateUserResponse(Some(user1)))
      assertUserAlreadyExists(res2)
      assertEquals(unsetResourceVersion(res3), CreateUserResponse(Some(user2)))
      assertEquals(res4, DeleteUserResponse())
      val resourceVersion1 = res1.user.value.metadata.value.resourceVersion
      assert(resourceVersion1.nonEmpty, "New user's resource version should be non empty")
      val resourceVersion2 = res3.user.value.metadata.value.resourceVersion
      assert(resourceVersion2.nonEmpty, "New user's resource version should be non empty")
    }
  }

  userManagementTest(
    shortIdentifier = "TestInvalidResourceVersionInCreateUser",
    description = "Exercise CreateUser rpc using resource version",
  ) { implicit ec => implicit ledger => _ =>
    val userId = ledger.nextUserId()
    val user = User(
      id = userId,
      primaryParty = "",
      metadata = Some(
        ObjectMeta(
          resourceVersion = "someResourceVersion1",
          annotations = Map.empty,
        )
      ),
      identityProviderId = "",
      isDeactivated = false,
    )
    for {
      res <- ledger
        .createUser(CreateUserRequest(Some(user), Nil))
        .mustFail(
          "creating user with non empty resource version"
        )
    } yield {
      assertGrpcError(
        res,
        RequestValidationErrors.InvalidArgument,
        exceptionMessageSubstring = Some("user.metadata.resource_version"),
      )
    }
  }

  userManagementTest(
    "TestGetUser",
    "Exercise GetUser rpc",
  ) { implicit ec => implicit ledger => _ =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = newUser(
      id = userId1,
      primaryParty = "party1",
    )
    for {
      _ <- ledger.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res1 <- ledger.userManagement.getUser(GetUserRequest(userId1, ""))
      res2 <- ledger.userManagement
        .getUser(GetUserRequest(userId2, ""))
        .mustFail("retrieving non-existent user")
    } yield {
      assertUserNotFound(res2)
      assert(unsetResourceVersion(res1) == GetUserResponse(Some(user1)))
    }
  }

  userManagementTest(
    "TestDeleteUser",
    "Exercise DeleteUser rpc",
  ) { implicit ec => implicit ledger => _ =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(
      id = userId1,
      primaryParty = "party1",
      identityProviderId = "",
      isDeactivated = false,
      metadata = None,
    )
    for {
      _ <- ledger.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res1 <- ledger.deleteUser(DeleteUserRequest(userId1, ""))
      res2 <- ledger.userManagement
        .deleteUser(DeleteUserRequest(userId2, ""))
        .mustFail("deleting non-existent user")
    } yield {
      assertEquals(res1, DeleteUserResponse())
      assertUserNotFound(res2)
    }
  }

  userManagementTest(
    "TestListUsersVisibilityOfNewUserWhenCreatedAndThenDeleted",
    "Exercise ListUsers rpc: Creating and deleting a user makes it visible and then absent from a page",
    runConcurrently = false,
  ) { implicit ec => implicit ledger => _ =>
    def assertUserPresentIn(user: User, list: ListUsersResponse, msg: String): Unit =
      assert(list.users.map(unsetResourceVersion).contains(user), msg)

    def assertUserAbsentIn(user: User, list: ListUsersResponse, msg: String): Unit =
      assert(!list.users.map(unsetResourceVersion).contains(user), msg)

    for {
      pageBeforeCreate <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = "", pageSize = 10, identityProviderId = "")
      )
      // Construct an user-id that with high probability will be the first on the first page
      // (Note: "!" is the smallest valid user-id character)
      newUserId = "!" + pageBeforeCreate.users.headOption
        .map(_.id)
        .getOrElse(ledger.nextUserId())
      newUser1 = newUser(
        id = newUserId,
        primaryParty = "",
      )
      _ = assertUserAbsentIn(
        newUser1,
        pageBeforeCreate,
        "new user should be absent before it's creation",
      )
      _ <- ledger.createUser(CreateUserRequest(Some(newUser1), Nil))
      pageAfterCreate <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = "", pageSize = 10, identityProviderId = "")
      )
      _ = assertUserPresentIn(
        newUser1,
        pageAfterCreate,
        "new users should be present after it's creation",
      )
      _ <- ledger.deleteUser(DeleteUserRequest(newUserId, identityProviderId = ""))
      pageAfterDelete <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = "", pageSize = 10, identityProviderId = "")
      )
      _ = assertUserAbsentIn(
        newUser1,
        pageAfterDelete,
        "new user should be absent after it's deletion",
      )
    } yield {
      ()
    }
  }

  userManagementTest(
    "TestListUsersCreateOrDeleteUserOnPreviousPage",
    "Exercise ListUsers rpc: Adding a user to a previous page doesn't affect the subsequent page",
    runConcurrently = false,
  ) { implicit ec => implicit ledger => _ =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val userId3 = ledger.nextUserId()
    val userId4 = ledger.nextUserId()

    for {
      // Create 4 users to ensure we have at least two pages of two users each
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userId1,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userId2,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userId3,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userId4,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      // Fetch the first two full pages
      page1 <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = "", pageSize = 2, identityProviderId = "")
      )
      page2 <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = page1.nextPageToken, pageSize = 2, identityProviderId = "")
      )
      // Verify that the second page stays the same even after we have created a new user that is lexicographically smaller than the last user on the first page
      // (Note: "!" is the smallest valid user-id character)
      newUserId = "!" + page1.users.lastOption.value.id
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = newUserId,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Seq.empty,
        )
      )
      page2B <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = page1.nextPageToken, pageSize = 2, identityProviderId = "")
      )
      _ = assertEquals("after creating new user before the second page", page2, page2B)
    } yield {
      ()
    }
  }

  userManagementTest(
    "TestListUsersReachingTheLastPage",
    "Exercise ListUsers rpc: Listing all users page by page eventually terminates reaching the last page",
  ) { implicit ec => implicit ledger => _ =>
    val pageSize = 10000

    def fetchNextPage(pageToken: String, pagesFetched: Int): Future[Unit] =
      for {
        page <- ledger.userManagement.listUsers(
          ListUsersRequest(pageSize = pageSize, pageToken = pageToken, identityProviderId = "")
        )
        _ = if (page.nextPageToken != "") {
          if (pagesFetched > 10) {
            fail(
              s"Could not reach the last page even after fetching ${pagesFetched + 1} pages of size $pageSize each"
            )
          }
          fetchNextPage(pageToken = page.nextPageToken, pagesFetched = pagesFetched + 1)
        }
      } yield ()

    fetchNextPage(pageToken = "", pagesFetched = 0)
  }

  userManagementTest(
    "TestListUsersOnInvalidRequests",
    "Exercise ListUsers rpc: Requesting invalid pageSize or pageToken results in an error",
  ) { implicit ec => implicit ledger => _ =>
    for {
      // Using not Base64 encoded string as the page token
      onBadTokenError <- ledger.userManagement
        .listUsers(
          ListUsersRequest(
            pageToken = UUID.randomUUID().toString,
            pageSize = 0,
            identityProviderId = "",
          )
        )
        .mustFail("using invalid page token string")
      // Using negative pageSize
      onNegativePageSizeError <- ledger.userManagement
        .listUsers(ListUsersRequest(pageSize = -100, pageToken = "", identityProviderId = ""))
        .mustFail("using negative page size")
    } yield {
      assertGrpcError(
        t = onBadTokenError,
        errorCode = RequestValidationErrors.InvalidArgument,
        exceptionMessageSubstring = None,
      )
      assertGrpcError(
        t = onNegativePageSizeError,
        errorCode = RequestValidationErrors.InvalidArgument,
        exceptionMessageSubstring = None,
      )
    }

  }

  userManagementTest(
    "TestListUsersRequestPageSizeZero",
    "Exercise ListUsers rpc: Requesting page of size zero means requesting server's default page size, which is larger than zero",
    runConcurrently = false,
  ) { implicit ec => implicit ledger => _ =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    for {
      // Ensure we have at least two users
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userId1,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      _ <- ledger.createUser(
        CreateUserRequest(
          Some(
            User(
              id = userId2,
              primaryParty = "",
              identityProviderId = "",
              isDeactivated = false,
              metadata = None,
            )
          ),
          Nil,
        )
      )
      pageSizeZero <- ledger.userManagement.listUsers(
        ListUsersRequest(pageSize = 0, pageToken = "", identityProviderId = "")
      )
      pageSizeOne <- ledger.userManagement.listUsers(
        ListUsersRequest(pageSize = 1, pageToken = "", identityProviderId = "")
      )
    } yield {
      assert(
        pageSizeOne.users.nonEmpty,
        "First page with requested pageSize zero should return some users",
      )
      assertEquals(pageSizeZero.users.headOption.value, pageSizeOne.users.headOption.value)
    }
  }

  test(
    "TestMaxUsersPageSize",
    "Exercise ListUsers rpc: Requesting more than maxUsersPageSize results in at most maxUsersPageSize returned users",
    allocate(NoParties),
    enabled = _.userManagement.maxUsersPageSize > 0,
    disabledReason = "requires user management feature with users page size limit",
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger, Seq())) =>
    val maxUsersPageSize = ledger.features.userManagement.maxUsersPageSize
    val users = 1
      .to(maxUsersPageSize + 1)
      .map(_ =>
        User(
          id = ledger.nextUserId(),
          primaryParty = "",
          isDeactivated = false,
          metadata = None,
          identityProviderId = "",
        )
      )
    for {
      // create lots of users
      _ <- Future.sequence(
        users.map(u => ledger.createUser(CreateUserRequest(Some(u), Nil)))
      )
      // request page size greater than the server's limit
      page <- ledger.userManagement
        .listUsers(
          ListUsersRequest(pageSize = maxUsersPageSize + 1, pageToken = "", identityProviderId = "")
        )
    } yield {
      assert(
        page.users.sizeIs <= maxUsersPageSize,
        s"page size must be within limit. actual size: ${page.users.size}, server's limit: $maxUsersPageSize",
      )
    }
  })

  userManagementTest(
    "TestGrantTheEmptyRight",
    "Test granting an empty right",
  ) { implicit ec => implicit ledger => _ =>
    val userId = ledger.nextUserId()
    val user = User(
      id = userId,
      primaryParty = "",
      identityProviderId = "",
      isDeactivated = false,
      metadata = None,
    )
    for {
      _ <- ledger.createUser(CreateUserRequest(Some(user), Nil))
      _ <- ledger.userManagement
        .grantUserRights(
          GrantUserRightsRequest(userId, List(Permission(Permission.Kind.Empty)), "")
        )
        .mustFailWith(
          "granting empty right",
          RequestValidationErrors.InvalidArgument,
          Some("unknown kind of right"),
        )
    } yield ()
  }

  userManagementTest(
    "TestGrantingAndRevokingEmptyListOfRights",
    "Test granting and revoking empty list of rights",
  ) { implicit ec => implicit ledger => _ =>
    val userId = ledger.nextUserId()
    val user = User(
      id = userId,
      primaryParty = "",
      identityProviderId = "",
      isDeactivated = false,
      metadata = None,
    )
    for {
      _ <- ledger.createUser(CreateUserRequest(Some(user), Nil))
      _ <- ledger.userManagement.grantUserRights(GrantUserRightsRequest(userId, List.empty, ""))
      _ <- ledger.userManagement.revokeUserRights(RevokeUserRightsRequest(userId, List.empty, ""))
    } yield ()
  }

  userManagementTest(
    "TestGrantUserRights",
    "Exercise GrantUserRights rpc",
  ) { implicit ec => implicit ledger => _ =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(
      id = userId1,
      primaryParty = "party1",
      identityProviderId = "",
      isDeactivated = false,
      metadata = None,
    )
    val suffix = UUID.randomUUID().toString
    for {
      parties <- allocateParties(ledger, suffix)
      userRights = getUserRights(parties)
      userBefore <- ledger.createUser(CreateUserRequest(Some(user1), Nil))
      res1 <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(userId1, List(adminPermission), "")
      )
      res2 <- ledger.userManagement
        .grantUserRights(GrantUserRightsRequest(userId2, List(adminPermission), ""))
        .mustFail("granting right to a non-existent user")
      res3 <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(userId1, List(adminPermission), "")
      )
      res4 <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(userId1, userRights, "")
      )
      userAfter <- ledger.userManagement.getUser(
        GetUserRequest(userId = user1.id, identityProviderId = "")
      )
    } yield {
      assertSameElements(res1.newlyGrantedRights, List(adminPermission))
      assertUserNotFound(res2)
      assertSameElements(res3.newlyGrantedRights, List.empty)
      assertSameElements(res4.newlyGrantedRights.toSet, userRights.toSet)
      val userResourceVersion1 = userBefore.user.value.metadata.value.resourceVersion
      val userResourceVersion2 = userAfter.user.value.metadata.value.resourceVersion
      assertEquals(
        "changing user rights must not change user's resource version",
        userResourceVersion1,
        userResourceVersion2,
      )
    }
  }

  userManagementTest(
    "TestAllocationTimeGrantUserRights",
    "Exercise AllocateParty rpc",
  ) { implicit ec => implicit ledger => _ =>
    val userId1 = ledger.nextUserId()
    val user1 = User(
      id = userId1,
      primaryParty = "party1",
      identityProviderId = "",
      isDeactivated = false,
      metadata = None,
    )
    val suffix = UUID.randomUUID().toString
    val party = s"acting-party-1-$suffix"
    for {
      res1 <- ledger
        .allocateParty(partyIdHint = Some(party), userId = user1.id)
        .mustFail("granting right to a non-existent user")
      _ <- ledger.createUser(CreateUserRequest(Some(user1), Nil))
      rightsBefore <- ledger.userManagement.listUserRights(ListUserRightsRequest(userId1, ""))
      allocated <- ledger.allocateParty(partyIdHint = Some(party), userId = user1.id)
      rightsAfter <- ledger.userManagement.listUserRights(ListUserRightsRequest(userId1, ""))
    } yield {
      assertUserNotFound(res1)
      assert(rightsBefore.rights.isEmpty, "rights should be empty")
      assertSameElements(
        rightsAfter.rights.toSet,
        Set(actAsPermission(allocated.underlying)),
      )
    }
  }

  private def getUserRights(parties: UserManagementServiceIT.Parties) =
    List(
      actAsPermission(parties.acting1),
      actAsPermission(parties.acting2),
      readAsPermission(parties.reading1),
      readAsPermission(parties.reading2),
      executeAsPermission(parties.executing1),
      executeAsPermission(parties.executing2),
    )

  userManagementTest(
    "TestRevokeUserRights",
    "Exercise RevokeUserRights rpc",
  ) { implicit ec => implicit ledger => _ =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(
      id = userId1,
      primaryParty = "party1",
      identityProviderId = "",
      isDeactivated = false,
      metadata = None,
    )
    val suffix = UUID.randomUUID().toString
    for {
      parties <- allocateParties(ledger, suffix)
      userRights = getUserRights(parties)
      userBefore <- ledger.createUser(
        CreateUserRequest(Some(user1), List(adminPermission) ++ userRights)
      )
      res1 <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(userId1, List(adminPermission), "")
      )
      res2 <- ledger.userManagement
        .revokeUserRights(RevokeUserRightsRequest(userId2, List(adminPermission), ""))
        .mustFail("revoking right from a non-existent user")
      res3 <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(userId1, List(adminPermission), "")
      )
      res4 <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(userId1, userRights, "")
      )
      userAfter <- ledger.userManagement.getUser(
        GetUserRequest(userId = user1.id, identityProviderId = "")
      )
    } yield {
      assertEquals(res1, RevokeUserRightsResponse(List(adminPermission)))
      assertUserNotFound(res2)
      assertSameElements(res3.newlyRevokedRights, List.empty)
      assertSameElements(res4.newlyRevokedRights.toSet, userRights.toSet)
      val userResourceVersion1 = userBefore.user.value.metadata.value.resourceVersion
      val userResourceVersion2 = userAfter.user.value.metadata.value.resourceVersion
      assertEquals(
        "changing user rights must not change user's resource version",
        userResourceVersion1,
        userResourceVersion2,
      )
    }
  }

  userManagementTest(
    "TestListUserRights",
    "Exercise ListUserRights rpc",
  ) { implicit ec => implicit ledger => _ =>
    val userId1 = ledger.nextUserId()
    val user1 = User(
      id = userId1,
      primaryParty = "party4",
      metadata = Some(ObjectMeta.defaultInstance),
      identityProviderId = "",
      isDeactivated = false,
    )
    val suffix = UUID.randomUUID().toString
    for {
      parties <- allocateParties(ledger, suffix)
      res1 <- ledger.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res2 <- ledger.userManagement.listUserRights(ListUserRightsRequest(userId1, ""))
      res3 <- ledger.userManagement.grantUserRights(
        GrantUserRightsRequest(
          userId1,
          List(
            adminPermission,
            actAsPermission(parties.acting1),
            readAsPermission(parties.reading1),
            executeAsPermission(parties.executing1),
          ),
          "",
        )
      )
      res4 <- ledger.userManagement.listUserRights(ListUserRightsRequest(userId1, ""))
      res5 <- ledger.userManagement.revokeUserRights(
        RevokeUserRightsRequest(userId1, List(adminPermission), "")
      )
      res6 <- ledger.userManagement
        .listUserRights(ListUserRightsRequest(userId1, ""))
    } yield {
      assertEquals(unsetResourceVersion(res1), CreateUserResponse(Some(user1)))
      assertEquals(res2, ListUserRightsResponse(Seq.empty))
      assertSameElements(
        res3.newlyGrantedRights.toSet,
        Set(
          adminPermission,
          actAsPermission(parties.acting1),
          readAsPermission(parties.reading1),
          executeAsPermission(parties.executing1),
        ),
      )
      assertSameElements(
        res4.rights.toSet,
        Set(
          adminPermission,
          actAsPermission(parties.acting1),
          readAsPermission(parties.reading1),
          executeAsPermission(parties.executing1),
        ),
      )
      assertSameElements(res5.newlyRevokedRights, Seq(adminPermission))
      assertSameElements(
        res6.rights.toSet,
        Set(
          actAsPermission(parties.acting1),
          readAsPermission(parties.reading1),
          executeAsPermission(parties.executing1),
        ),
      )
    }
  }

  def allocateParties(ledger: ParticipantTestContext, suffix: String)(implicit
      ec: ExecutionContext
  ): Future[UserManagementServiceIT.Parties] =
    for {
      acting1 <- ledger.allocateParty(Some(s"acting-party-1-$suffix"))
      acting2 <- ledger.allocateParty(Some(s"acting-party-2-$suffix"))
      reading1 <- ledger.allocateParty(Some(s"reading-party-1-$suffix"))
      reading2 <- ledger.allocateParty(Some(s"reading-party-2-$suffix"))
      executing1 <- ledger.allocateParty(Some(s"executing-party-1-$suffix"))
      executing2 <- ledger.allocateParty(Some(s"executing-party-2-$suffix"))
    } yield UserManagementServiceIT.Parties(
      acting1,
      acting2,
      reading1,
      reading2,
      executing1,
      executing2,
    )
}

object UserManagementServiceIT {
  final case class Parties(
      acting1: String,
      acting2: String,
      reading1: String,
      reading2: String,
      executing1: String,
      executing2: String,
  )
}
