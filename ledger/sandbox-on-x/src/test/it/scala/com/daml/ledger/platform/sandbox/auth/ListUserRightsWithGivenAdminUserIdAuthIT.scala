// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service._
import io.grpc.{Status, StatusRuntimeException}

import java.util.UUID
import scala.concurrent.Future

class ListUserRightsWithGivenAdminUserIdAuthIT
    extends AdminServiceCallAuthTests
    with UserManagementAuth {

  override def serviceCallName: String = "UserManagementService#ListUserRights(given-user-id)"

  // admin and idp admin users are allowed to specify a user-id other than their own for which to retrieve a user
  override def serviceCall(context: ServiceCallContext): Future[Any] = {
    val testId = UUID.randomUUID().toString

    def getRights(userId: String): Future[ListUserRightsResponse] =
      stub(context.token).listUserRights(ListUserRightsRequest(userId))

    for {
      // create a normal users
      (alice, _) <- createUserByAdmin(testId + "-alice")

      // test that only admins can retrieve his own user and the newly created alice user
      _ <- getRights("participant_admin")
      _ <- getRights(alice.id)

      // test for a non-existent user
      _ <- stub(UserManagementServiceGrpc.stub(channel), context.token)
        .listUserRights(ListUserRightsRequest("non-existent-user-" + UUID.randomUUID().toString))
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
