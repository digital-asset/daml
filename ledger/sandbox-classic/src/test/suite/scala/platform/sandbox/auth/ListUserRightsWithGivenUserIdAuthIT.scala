// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.util.UUID

import com.daml.ledger.api.v1.admin.user_management_service._
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.Future

class ListUserRightsWithGivenUserIdAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "UserManagementService#ListUserRights(given-user-id)"

  // only admin users are allowed to specify a user-id for which to retrieve rights
  override def serviceCallWithToken(token: Option[String]): Future[Any] = {
    for {
      // test for an existing user
      _ <- stub(UserManagementServiceGrpc.stub(channel), token)
        .listUserRights(ListUserRightsRequest("participant_admin"))
      // test for a non-existent user
      _ <- stub(UserManagementServiceGrpc.stub(channel), token)
        .listUserRights(ListUserRightsRequest("non-existent-user-" + UUID.randomUUID().toString))
        // FIXME: express this better using Scalatest expectations
        .transform({
          case scala.util.Success(u) =>
            scala.util.Failure(new RuntimeException(s"User $u unexpectedly exists."))
          case scala.util.Failure(e: StatusRuntimeException)
              if (e.getStatus.getCode == Status.Code.NOT_FOUND) =>
            scala.util.Success(())
          case scala.util.Failure(e: Throwable) => scala.util.Failure(e)
        })
    } yield ()
  }

}
