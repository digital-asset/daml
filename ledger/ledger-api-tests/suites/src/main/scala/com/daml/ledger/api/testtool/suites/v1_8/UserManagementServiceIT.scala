// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import java.util.UUID

import com.daml.error.ErrorCode
import com.daml.error.definitions.{IndexErrors, LedgerApiErrors}
import com.daml.error.utils.ErrorDetails
import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantTestContext
import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  CreateUserResponse,
  DeleteUserRequest,
  DeleteUserResponse,
  GetUserRequest,
  GetUserResponse,
  GrantUserRightsRequest,
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

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

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
        t = t,
        errorCode = LedgerApiErrors.AdminServices.TooManyUserRights,
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
      assertEquals(create2.user, Some(user1))
      assertTooManyUserRightsError(grant1)
      assertEquals(rights1.rights.size, permissionsMaxAndOne.tail.size)
      assertSameElements(rights1.rights, permissionsMaxAndOne.tail)
      assertEquals(create3.user, Some(user2))
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
    "CreateUsersRaceCondition",
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
          val unexpectedErrors = results
            .collect { case x if x.isFailure => x.failed.get }
            .filterNot(t =>
              ErrorDetails.matches(t, LedgerApiErrors.AdminServices.UserAlreadyExists) ||
                ErrorDetails.matches(t, IndexErrors.DatabaseErrors.SqlTransientError) ||
                ErrorDetails.isInternalError(t)
            )
          assertSameElements(actual = unexpectedErrors, expected = Seq.empty)
        }
      }
  }

  userManagementTest(
    "GrantRightsRaceCondition",
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
          assert(
            results.exists(_.isSuccess),
            "Expected at least one successful user right grant",
          )
          val unexpectedErrors = results
            .collect { case x if x.isFailure => x.failed.get }
            // Note: `IndexErrors.DatabaseErrors.SqlNonTransientError` is signalled on H2 and the original cause being `org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException`
            .filterNot(e =>
              ErrorDetails.isInternalError(e) || ErrorDetails
                .matches(e, IndexErrors.DatabaseErrors.SqlTransientError)
            )
          assertSameElements(actual = unexpectedErrors, expected = Seq.empty)
        }
      }
  }

  userManagementTest(
    "TestAdminExists",
    "Ensure admin user exists",
  )(implicit ec => { implicit ledger =>
    for {
      get1 <- ledger.userManagement.getUser(GetUserRequest(AdminUserId))
      rights1 <- ledger.userManagement.listUserRights(ListUserRightsRequest(AdminUserId))
    } yield {
      assertEquals(get1.user, Some(User(AdminUserId, "")))
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
      res1 <- ledger.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res2 <- ledger
        .createUser(CreateUserRequest(Some(user1), Nil))
        .mustFail("allocating a duplicate user")
      res3 <- ledger.createUser(CreateUserRequest(Some(user2), Nil))
      res4 <- ledger.deleteUser(DeleteUserRequest(userId2))
    } yield {
      assertEquals(res1, CreateUserResponse(Some(user1)))
      assertUserAlreadyExists(res2)
      assertEquals(res3, CreateUserResponse(Some(user2)))
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
      _ <- ledger.createUser(
        CreateUserRequest(Some(user1), Nil)
      )
      res1 <- ledger.userManagement.getUser(GetUserRequest(userId1))
      res2 <- ledger.userManagement
        .getUser(GetUserRequest(userId2))
        .mustFail("retrieving non-existent user")
    } yield {
      assertUserNotFound(res2)
      assert(res1 == GetUserResponse(Some(user1)))
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
      assert(list.users.contains(user), msg)
    }

    def assertUserAbsentIn(user: User, list: ListUsersResponse, msg: String): Unit = {
      assert(!list.users.contains(user), msg)
    }

    for {
      pageBeforeCreate <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = "", pageSize = 10)
      )
      // Construct an user-id that with high probability will be the first on the first page
      // (Note: "!" is the smallest valid user-id character)
      newUserId = "!" + pageBeforeCreate.users.headOption.map(_.id).getOrElse(ledger.nextUserId())
      newUser = User(newUserId, "")
      _ = assertUserAbsentIn(
        newUser,
        pageBeforeCreate,
        "new user should be absent before it's creation",
      )
      _ <- ledger.createUser(CreateUserRequest(Some(newUser), Nil))
      pageAfterCreate <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = "", pageSize = 10)
      )
      _ = assertUserPresentIn(
        newUser,
        pageAfterCreate,
        "new users should be present after it's creation",
      )
      _ <- ledger.deleteUser(DeleteUserRequest(newUserId))
      pageAfterDelete <- ledger.userManagement.listUsers(
        ListUsersRequest(pageToken = "", pageSize = 10)
      )
      _ = assertUserAbsentIn(
        newUser,
        pageAfterDelete,
        "new user should be absent after it's delteion",
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
    "TestGrantUserRights",
    "Exercise GrantUserRights rpc",
  )(implicit ec => { implicit ledger =>
    val userId1 = ledger.nextUserId()
    val userId2 = ledger.nextUserId()
    val user1 = User(userId1, "party1")
    for {
      _ <- ledger.createUser(CreateUserRequest(Some(user1), Nil))
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
      _ <- ledger.createUser(
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
      assertEquals(res1.user, Some(user1))
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
      runConcurrently: Boolean = true,
  )(
      body: ExecutionContext => ParticipantTestContext => Future[Unit]
  ): Unit = {
    test(
      shortIdentifier = shortIdentifier,
      description = description,
      allocate(NoParties),
      enabled = _.userManagement.supported,
      disabledReason = "requires user management feature",
      runConcurrently = runConcurrently,
    )(implicit ec => { case Participants(Participant(ledger)) =>
      body(ec)(ledger)
    })
  }

  private def assertUserNotFound(t: Throwable): Unit = {
    assertGrpcError(
      t = t,
      errorCode = LedgerApiErrors.AdminServices.UserNotFound,
      exceptionMessageSubstring = None,
    )
  }

  private def assertUserAlreadyExists(
      t: Throwable
  ): Unit = {
    assertGrpcError(
      t = t,
      errorCode = LedgerApiErrors.AdminServices.UserAlreadyExists,
      exceptionMessageSubstring = None,
    )
  }

}
