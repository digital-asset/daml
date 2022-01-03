// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth
import java.util.UUID

import com.daml.ledger.api.v1.admin.user_management_service.{
  GetUserRequest,
  User,
  UserManagementServiceGrpc,
}
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests covering the special behaviour of GetUser wrt the authenticated user. */
class GetAuthenticatedUserAuthIT extends ServiceCallAuthTests {
  private val testId = UUID.randomUUID().toString

  override def serviceCallName: String = "UserManagementService#GetUser(<authenticated-user>)"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(UserManagementServiceGrpc.stub(channel), token).getUser(GetUserRequest())

  private def expectUser(token: Option[String], expectedUser: User): Future[Assertion] =
    serviceCallWithToken(token).map(assertResult(expectedUser)(_))

  private def getUser(token: Option[String], userId: String) =
    stub(UserManagementServiceGrpc.stub(channel), token).getUser(GetUserRequest(userId))

  behavior of serviceCallName

  it should "deny unauthenticated access" in {
    expectUnauthenticated(serviceCallWithToken(None))
  }

  it should "deny access for a standard token referring to an unknown user" in {
    expectPermissionDenied(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }

  it should "return the 'participant_admin' user when using its standard token" in {
    expectUser(canReadAsAdminStandardJWT, User("participant_admin", ""))
  }

  it should "return invalid argument for custom token" in {
    expectInvalidArgument(serviceCallWithToken(canReadAsAdmin))
  }

  it should "allow access to a non-admin user's own user record" in {
    for {
      // admin creates user
      (alice, aliceToken) <- createUserByAdmin(testId + "-alice")
      // user accesses its own user record without specifying the id
      aliceRetrieved1 <- getUser(aliceToken, "")
      // user accesses its own user record with specifying the id
      aliceRetrieved2 <- getUser(aliceToken, alice.id)

    } yield assertResult((alice, alice))((aliceRetrieved1, aliceRetrieved2))
  }
}
