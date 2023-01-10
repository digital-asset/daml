// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.util.UUID

import com.daml.error.ErrorCode
import com.daml.error.definitions.{IndexErrors, LedgerApiErrors}
import com.daml.error.utils.ErrorDetails
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions.{assertEquals, _}
import com.daml.ledger.api.v1.admin.object_meta.ObjectMeta
import com.daml.ledger.api.v1.admin.user_management_service.{
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
  User,
  Right => Permission,
}
import com.daml.ledger.api.v1.admin.{user_management_service => proto}

import scala.concurrent.Future
import scala.util.{Failure, Success}

final class UserManagementServiceIT extends UserManagementServiceITBase {

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
        t = t,
        errorCode = LedgerApiErrors.Admin.UserManagement.TooManyUserRights,
        exceptionMessageSubstring = None,
      )
    }

    def createCanActAs(id: Int) =
      Permission(Permission.Kind.CanActAs(Permission.CanActAs(s"acting-party-$id")))

    val user1 = newUser(UUID.randomUUID.toString)
    val user2 = newUser(UUID.randomUUID.toString)

    val maxRightsPerUser = ledger.features.userManagement.maxRightsPerUser
    val permissionsMaxAndOne = (1 to (maxRightsPerUser + 1)).map(createCanActAs)
    val permissionOne = permissionsMaxAndOne.head
    val permissionsMax = permissionsMaxAndOne.tail

    for {
      // cannot create user with #limit+1 rights
      create1 <- ledger
        .createUser(CreateUserRequest(Some(user1), permissionsMaxAndOne))
        .mustFail(
          "creating user with too many rights"
        )
      // can create user with #limit rights
      create2 <- ledger.createUser(CreateUserRequest(Some(user1), permissionsMax))
      // fails adding one more right
      grant1 <- ledger.userManagement
        .grantUserRights(GrantUserRightsRequest(user1.id, rights = Seq(permissionOne)))
        .mustFail(
          "granting more rights exceeds max number of user rights per user"
        )
      // rights already added are intact
      rights1 <- ledger.userManagement.listUserRights(ListUserRightsRequest(user1.id))
      // can create other users with #limit rights
      create3 <- ledger.createUser(CreateUserRequest(Some(user2), permissionsMax))
    } yield {
      assertTooManyUserRightsError(create1)
      assertEquals(unsetResourceVersion(create2), CreateUserResponse(Some(user1)))
      assertTooManyUserRightsError(grant1)
      assertEquals(rights1.rights.size, permissionsMaxAndOne.tail.size)
      assertSameElements(rights1.rights, permissionsMaxAndOne.tail)
      assertEquals(unsetResourceVersion(create3), CreateUserResponse(Some(user2)))
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
        throwable <- ledger
          .createUser(CreateUserRequest(Some(user), rights))
          .mustFail(context = problem)
      } yield assertGrpcError(
        t = throwable,
        errorCode = expectedErrorCode,
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
      } yield assertGrpcError(error, expectedErrorCode, None)

    for {
      _ <- getAndCheck("empty user-id", "", LedgerApiErrors.RequestValidation.InvalidArgument)
      _ <- getAndCheck("invalid user-id", "?", LedgerApiErrors.RequestValidation.InvalidField)
    } yield ()
  })

  userManagementTest(
    "RaceConditionCreateUsers",
    "Tests scenario of multiple concurrent create-user calls for the same user",
    runConcurrently = false,
  ) {
    implicit ec =>
      { participant =>
        val attempts = (1 to 10).toVector
        val userId = participant.nextUserId()
        val request =
          CreateUserRequest(Some(User(id = userId, primaryParty = "")), rights = Seq.empty)
        for {
          results <- Future
            .traverse(attempts) { _ =>
              participant.createUser(request).transform(Success(_))
            }
        } yield {
          assertSingleton(
            "successful user creation",
            results.filter(_.isSuccess),
          )
          val unexpectedErrors = results.collect {
            case Failure(t)
                if !ErrorDetails.matchesOneOf(
                  t,
                  IndexErrors.DatabaseErrors.SqlTransientError,
                  LedgerApiErrors.Admin.UserManagement.UserAlreadyExists,
                )
                  && !ErrorDetails.isInternalError(t) =>
              t
          }
          assertIsEmpty(unexpectedErrors)
        }
      }
  }

  userManagementTest(
    "RaceConditionDeleteUsers",
    "Tests scenario of multiple concurrent delete-user calls for the same user",
    runConcurrently = false,
  ) {
    implicit ec =>
      { participant =>
        val attempts = (1 to 10).toVector
        val userId = participant.nextUserId()
        val createUserRequest =
          CreateUserRequest(Some(User(id = userId, primaryParty = "")), rights = Seq.empty)
        val deleteUserRequest = DeleteUserRequest(userId = userId)
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
          )
          val unexpectedErrors = results
            .collect {
              case Failure(t)
                  if !ErrorDetails.matchesOneOf(
                    t,
                    IndexErrors.DatabaseErrors.SqlTransientError,
                    LedgerApiErrors.Admin.UserManagement.UserNotFound,
                  ) && !ErrorDetails.isInternalError(t) =>
                t
            }
          assertIsEmpty(unexpectedErrors)
        }
      }
  }

  userManagementTest(
    "RaceConditionGrantRights",
    "Tests scenario of multiple concurrent grant-right calls for the same user and the same rights",
    runConcurrently = false,
  ) {
    implicit ec =>
      { participant =>
        val attempts = (1 to 10).toVector
        val userId = participant.nextUserId()
        val createUserRequest =
          CreateUserRequest(Some(User(id = userId, primaryParty = "")), rights = Seq.empty)
        val grantRightsRequest = GrantUserRightsRequest(userId = userId, rights = userRightsBatch)
        for {
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
          assertSingleton("Success response", successes)
          assertSameElements(successes.head.newlyGrantedRights, userRightsBatch)
          val unexpectedErrors = results
            .collect {
              case Failure(t)
                  if
                  // Transient db error caused by unique constraint violation on H2
                  !ErrorDetails.matches(t, IndexErrors.DatabaseErrors.SqlTransientError)
                    &&
                      // Internal error caused by unique constraint violation on Postgres
                      !ErrorDetails.isInternalError(t) =>
                t
            }
          assertIsEmpty(unexpectedErrors)
        }
      }
  }

  userManagementTest(
    "RaceConditionRevokeRights",
    "Tests scenario of multiple concurrent revoke-right calls for the same user and the same rights",
    runConcurrently = false,
  ) {
    implicit ec =>
      { participant =>
        val attempts = (1 to 10).toVector
        val userId = participant.nextUserId()
        val createUserRequest =
          CreateUserRequest(Some(User(id = userId, primaryParty = "")), rights = userRightsBatch)
        val revokeRightsRequest = RevokeUserRightsRequest(userId = userId, rights = userRightsBatch)
        for {
          _ <- participant.createUser(createUserRequest)
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
          )
          assertIsEmpty(results.filter(_.isFailure))
        }
      }
  }

  userManagementTest(
    "TestAdminExists",
    "Ensure admin user exists",
  )(implicit ec => { implicit ledger =>
    val useMeta = ledger.features.userAndPartyLocalMetadataExtensions
    for {
      get1 <- ledger.userManagement.getUser(GetUserRequest(AdminUserId))
      rights1 <- ledger.userManagement.listUserRights(ListUserRightsRequest(AdminUserId))
    } yield {
      assertEquals(
        get1.user,
        Some(
          User(
            id = AdminUserId,
            metadata = Option.when(useMeta)(
              ObjectMeta(
                resourceVersion = get1.getUser.getMetadata.resourceVersion
              )
            ),
          )
        ),
      )
      assertEquals(rights1, ListUserRightsResponse(Seq(adminPermission)))
    }
  })

  userManagementTest(
    "TestCreateUser",
    "Exercise CreateUser rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(
      id = userId1,
      primaryParty = "party1",
      metadata = Some(ObjectMeta()),
    )
    val user2 = User(
      id = userId2,
      primaryParty = "",
      metadata = Some(ObjectMeta()),
    )
    for {
      res1 <- ledger.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res2 <- ledger
        .createUser(CreateUserRequest(Some(user1), Nil))
        .mustFail("allocating a duplicate user")
      res3 <- ledger.createUser(CreateUserRequest(Some(user2), Nil))
      res4 <- ledger.deleteUser(DeleteUserRequest(userId2))
    } yield {
      assertEquals(unsetResourceVersion(res1), CreateUserResponse(Some(user1)))
      assertUserAlreadyExists(res2)
      assertEquals(unsetResourceVersion(res3), CreateUserResponse(Some(user2)))
      assertEquals(res4, DeleteUserResponse())
      if (ledger.features.userAndPartyLocalMetadataExtensions) {
        val resourceVersion1 = res1.user.get.metadata.get.resourceVersion
        assert(resourceVersion1.nonEmpty, "New user's resource version should be non empty")
        val resourceVersion2 = res3.user.get.metadata.get.resourceVersion
        assert(resourceVersion2.nonEmpty, "New user's resource version should be non empty")
      }
    }
  })

  userManagementTest(
    shortIdentifier = "TestInvalidResourceVersionInCreateUser",
    description = "Exercise CreateUser rpc using resource version",
    requiresUserAndPartyLocalMetadataExtensions = true,
  )(implicit ec => { implicit ledger =>
    val userId = ledger.nextUserId()
    val user = User(
      id = userId,
      primaryParty = "",
      metadata = Some(
        ObjectMeta(
          resourceVersion = "someResourceVersion1"
        )
      ),
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
        LedgerApiErrors.RequestValidation.InvalidArgument,
        exceptionMessageSubstring = Some("user.metadata.resource_version"),
      )
    }
  })

  userManagementTest(
    "TestGetUser",
    "Exercise GetUser rpc",
  )(implicit ec => { implicit ledger =>
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
      res1 <- ledger.userManagement.getUser(GetUserRequest(userId1))
      res2 <- ledger.userManagement
        .getUser(GetUserRequest(userId2))
        .mustFail("retrieving non-existent user")
    } yield {
      assertUserNotFound(res2)
      assert(unsetResourceVersion(res1) == GetUserResponse(Some(user1)))
    }
  })

  userManagementTest(
    "TestDeleteUser",
    "Exercise DeleteUser rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(
      id = userId1,
      primaryParty = "party1",
    )
    for {
      _ <- ledger.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res1 <- ledger.deleteUser(DeleteUserRequest(userId1))
      res2 <- ledger.userManagement
        .deleteUser(DeleteUserRequest(userId2))
        .mustFail("deleting non-existent user")
    } yield {
      assertEquals(res1, DeleteUserResponse())
      assertUserNotFound(res2)
    }
  })

  userManagementTest(
    "TestListUsersVisibilityOfNewUserWhenCreatedAndThenDeleted",
    "Exercise ListUsers rpc: Creating and deleting a user makes it visible and then absent from a page",
    runConcurrently = false,
  )(implicit ec => { implicit ledger =>
    def assertUserPresentIn(user: User, list: ListUsersResponse, msg: String): Unit = {
      assert(list.users.map(unsetResourceVersion).contains(user), msg)
    }

    def assertUserAbsentIn(user: User, list: ListUsersResponse, msg: String): Unit = {
      assert(!list.users.map(unsetResourceVersion).contains(user), msg)
    }

    for {
      pageBeforeCreate <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = "", pageSize = 10)
      )
      // Construct an user-id that with high probability will be the first on the first page
      // (Note: "!" is the smallest valid user-id character)
      newUserId = "!" + pageBeforeCreate.users.headOption.map(_.id).getOrElse(ledger.nextUserId())
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
        ListUsersRequest(pageToken = "", pageSize = 10)
      )
      _ = assertUserPresentIn(
        newUser1,
        pageAfterCreate,
        "new users should be present after it's creation",
      )
      _ <- ledger.deleteUser(DeleteUserRequest(newUserId))
      pageAfterDelete <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = "", pageSize = 10)
      )
      _ = assertUserAbsentIn(
        newUser1,
        pageAfterDelete,
        "new user should be absent after it's deletion",
      )
    } yield {
      ()
    }
  })

  userManagementTest(
    "TestListUsersCreateOrDeleteUserOnPreviousPage",
    "Exercise ListUsers rpc: Adding a user to a previous page doesn't affect the subsequent page",
    runConcurrently = false,
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val userId3 = ledger.nextUserId()
    val userId4 = ledger.nextUserId()

    for {
      // Create 4 users to ensure we have at least two pages of two users each
      _ <- ledger.createUser(CreateUserRequest(Some(User(userId1, "")), Nil))
      _ <- ledger.createUser(CreateUserRequest(Some(User(userId2, "")), Nil))
      _ <- ledger.createUser(CreateUserRequest(Some(User(userId3, "")), Nil))
      _ <- ledger.createUser(CreateUserRequest(Some(User(userId4, "")), Nil))
      // Fetch the first two full pages
      page1 <- ledger.userManagement.listUsers(ListUsersRequest(pageToken = "", pageSize = 2))
      page2 <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = page1.nextPageToken, pageSize = 2)
      )
      // Verify that the second page stays the same even after we have created a new user that is lexicographically smaller than the last user on the first page
      // (Note: "!" is the smallest valid user-id character)
      newUserId = "!" + page1.users.last.id
      _ <- ledger.createUser(CreateUserRequest(Some(User(newUserId)), Seq.empty))
      page2B <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = page1.nextPageToken, pageSize = 2)
      )
      _ = assertEquals("after creating new user before the second page", page2, page2B)
    } yield {
      ()
    }
  })

  userManagementTest(
    "TestListUsersReachingTheLastPage",
    "Exercise ListUsers rpc: Listing all users page by page eventually terminates reaching the last page",
  )(implicit ec => { implicit ledger =>
    val pageSize = 10000

    def fetchNextPage(pageToken: String, pagesFetched: Int): Future[Unit] = {
      for {
        page <- ledger.userManagement.listUsers(
          ListUsersRequest(pageSize = pageSize, pageToken = pageToken)
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
    }

    fetchNextPage(pageToken = "", pagesFetched = 0)
  })

  userManagementTest(
    "TestListUsersOnInvalidRequests",
    "Exercise ListUsers rpc: Requesting invalid pageSize or pageToken results in an error",
  )(implicit ec => { implicit ledger =>
    for {
      // Using not Base64 encoded string as the page token
      onBadTokenError <- ledger.userManagement
        .listUsers(ListUsersRequest(pageToken = UUID.randomUUID().toString))
        .mustFail("using invalid page token string")
      // Using negative pageSize
      onNegativePageSizeError <- ledger.userManagement
        .listUsers(ListUsersRequest(pageSize = -100))
        .mustFail("using negative page size")
    } yield {
      assertGrpcError(
        t = onBadTokenError,
        errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
        exceptionMessageSubstring = None,
      )
      assertGrpcError(
        t = onNegativePageSizeError,
        errorCode = LedgerApiErrors.RequestValidation.InvalidArgument,
        exceptionMessageSubstring = None,
      )
    }

  })

  userManagementTest(
    "TestListUsersRequestPageSizeZero",
    "Exercise ListUsers rpc: Requesting page of size zero means requesting server's default page size, which is larger than zero",
    runConcurrently = false,
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    for {
      // Ensure we have at least two users
      _ <- ledger.createUser(CreateUserRequest(Some(User(userId1, "")), Nil))
      _ <- ledger.createUser(CreateUserRequest(Some(User(userId2, "")), Nil))
      pageSizeZero <- ledger.userManagement.listUsers(
        ListUsersRequest(pageSize = 0)
      )
      pageSizeOne <- ledger.userManagement.listUsers(
        ListUsersRequest(pageSize = 1)
      )
    } yield {
      assert(
        pageSizeOne.users.nonEmpty,
        "First page with requested pageSize zero should return some users",
      )
      assertEquals(pageSizeZero.users.head, pageSizeOne.users.head)
    }
  })

  test(
    "TestMaxUsersPageSize",
    "Exercise ListUsers rpc: Requesting more than maxUsersPageSize results in at most maxUsersPageSize returned users",
    allocate(NoParties),
    enabled = _.userManagement.maxUsersPageSize > 0,
    disabledReason = "requires user management feature with users page size limit",
    runConcurrently = false,
  )(implicit ec => { case Participants(Participant(ledger)) =>
    val maxUsersPageSize = ledger.features.userManagement.maxUsersPageSize
    val users = 1.to(maxUsersPageSize + 1).map(_ => User(ledger.nextUserId(), ""))
    for {
      // create lots of users
      _ <- Future.sequence(
        users.map(u => ledger.createUser(CreateUserRequest(Some(u), Nil)))
      )
      // request page size greater than the server's limit
      page <- ledger.userManagement
        .listUsers(
          ListUsersRequest(pageSize = maxUsersPageSize + 1, pageToken = "")
        )
    } yield {
      assert(
        page.users.size <= maxUsersPageSize,
        s"page size must be within limit. actual size: ${page.users.size}, server's limit: $maxUsersPageSize",
      )
    }
  })

  userManagementTest(
    "TestGrantTheEmptyRight",
    "Test granting an empty right",
  )(implicit ec => { implicit ledger =>
    val userId = ledger.nextUserId()
    val user = User(userId)
    for {
      _ <- ledger.createUser(CreateUserRequest(Some(user), Nil))
      _ <- ledger.userManagement
        .grantUserRights(
          GrantUserRightsRequest(userId, List(Permission(Permission.Kind.Empty)))
        )
        .mustFailWith(
          "granting empty right",
          LedgerApiErrors.RequestValidation.InvalidArgument,
          Some("unknown kind of right"),
        )
    } yield ()
  })

  userManagementTest(
    "TestGrantingAndRevokingEmptyListOfRights",
    "Test granting and revoking empty list of rights",
  )(implicit ec => { implicit ledger =>
    val userId = ledger.nextUserId()
    val user = User(userId)
    for {
      _ <- ledger.createUser(CreateUserRequest(Some(user), Nil))
      _ <- ledger.userManagement.grantUserRights(GrantUserRightsRequest(userId, List.empty))
      _ <- ledger.userManagement.revokeUserRights(RevokeUserRightsRequest(userId, List.empty))
    } yield ()
  })

  userManagementTest(
    "TestGrantUserRights",
    "Exercise GrantUserRights rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(userId1, "party1")
    for {
      userBefore <- ledger.createUser(CreateUserRequest(Some(user1), Nil))
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
      userAfter <- ledger.userManagement.getUser(GetUserRequest(userId = user1.id))
    } yield {
      assertSameElements(res1.newlyGrantedRights, List(adminPermission))
      assertUserNotFound(res2)
      assertSameElements(res3.newlyGrantedRights, List.empty)
      assertSameElements(res4.newlyGrantedRights.toSet, userRightsBatch.toSet)
      if (ledger.features.userAndPartyLocalMetadataExtensions) {
        val userResourceVersion1 = userBefore.user.get.metadata.get.resourceVersion
        val userResourceVersion2 = userAfter.user.get.metadata.get.resourceVersion
        assertEquals(
          "changing user rights must not change user's resource version",
          userResourceVersion1,
          userResourceVersion2,
        )
      }
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
      userBefore <- ledger.createUser(
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
      userAfter <- ledger.userManagement.getUser(GetUserRequest(userId = user1.id))
    } yield {
      assertEquals(res1, RevokeUserRightsResponse(List(adminPermission)))
      assertUserNotFound(res2)
      assertSameElements(res3.newlyRevokedRights, List.empty)
      assertSameElements(res4.newlyRevokedRights.toSet, userRightsBatch.toSet)
      if (ledger.features.userAndPartyLocalMetadataExtensions) {
        val userResourceVersion1 = userBefore.user.get.metadata.get.resourceVersion
        val userResourceVersion2 = userAfter.user.get.metadata.get.resourceVersion
        assertEquals(
          "changing user rights must not change user's resource version",
          userResourceVersion1,
          userResourceVersion2,
        )
      }
    }
  })

  userManagementTest(
    "TestListUserRights",
    "Exercise ListUserRights rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val user1 = User(
      id = userId1,
      primaryParty = "party4",
      metadata = Some(ObjectMeta()),
    )
    for {
      res1 <- ledger.createUser(
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
      assertEquals(unsetResourceVersion(res1), CreateUserResponse(Some(user1)))
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

}
