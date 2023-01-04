// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.util.UUID

import com.daml.ledger.api.v1.admin.user_management_service._
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.Future

class GetUserWithGivenUserIdAuthIT extends AdminServiceCallAuthTests {
  override def serviceCallName: String = "UserManagementService#GetUser(given-user-id)"

  // only admin users are allowed to specify a user-id other than their own for which to retrieve a user
  override def serviceCallWithToken(token: Option[String]): Future[Any] = {
    val testId = UUID.randomUUID().toString

    def getUser(userId: String): Future[User] =
      stub(UserManagementServiceGrpc.stub(channel), token)
        .getUser(GetUserRequest(userId))
        .map(_.user.get)

    for {
      // create a normal users
      (alice, _) <- createUserByAdmin(testId + "-alice")

      // test that only admins can retrieve his own user and the newly created alice user
      _ <- getUser("participant_admin")
      _ <- getUser(alice.id)

      // test for a non-existent user
      _ <- getUser("non-existent-user-" + testId)
        .transform({
          case scala.util.Success(u) =>
            scala.util.Failure(new RuntimeException(s"User $u unexpectedly exists."))
          case scala.util.Failure(e: StatusRuntimeException)
              if e.getStatus.getCode == Status.Code.NOT_FOUND =>
            scala.util.Success(())
          case scala.util.Failure(e: Throwable) => scala.util.Failure(e)
        })
    } yield ()
  }
}
