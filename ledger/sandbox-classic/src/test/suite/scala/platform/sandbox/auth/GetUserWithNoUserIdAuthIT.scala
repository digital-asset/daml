// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth
import com.daml.ledger.api.v1.admin.user_management_service.{
  GetUserRequest,
  User,
  UserManagementServiceGrpc,
}
import org.scalatest.Assertion

import scala.concurrent.Future

/** Tests covering the special behaviour of GetUser when not specifying a user-id. */
class GetUserWithNoUserIdAuthIT extends ServiceCallAuthTests {
  override def serviceCallName: String = "UserManagementService#GetUser(<no-user-id>)"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(UserManagementServiceGrpc.stub(channel), token).getUser(GetUserRequest())

  protected def expectUser(token: Option[String], expectedUser: User): Future[Assertion] =
    serviceCallWithToken(token).map(assertResult(expectedUser)(_))

  behavior of serviceCallName

  it should "deny unauthenticated access" in {
    expectUnauthenticated(serviceCallWithToken(None))
  }

  it should "deny access for a standard token referring to an unknown user" in {
    expectUnauthenticated(serviceCallWithToken(canReadAsUnknownUserStandardJWT))
  }

  it should "return the 'participant_admin' user when using its standard token" in {
    expectUser(canReadAsAdminStandardJWT, User("participant_admin", ""))
  }

  it should "return invalid argument for custom token" in {
    expectInvalidArgument(serviceCallWithToken(canReadAsAdmin))
  }
}
