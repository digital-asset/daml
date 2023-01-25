// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import java.util.UUID

import com.daml.ledger.api.v1.admin.user_management_service._
import io.grpc.{Status, StatusRuntimeException}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
import scala.concurrent.Future

class GetUserWithGivenUserIdAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with UserManagementAuth {
  override def serviceCallName: String = "UserManagementService#GetUser(given-user-id)"

  // admin and idp admin users are allowed to specify a user-id other than their own for which to retrieve a user
  override def serviceCall(context: ServiceCallContext): Future[Any] = {
    val testId = UUID.randomUUID().toString

    def getUser(userId: String): Future[User] =
      stub(UserManagementServiceGrpc.stub(channel), context.token)
        .getUser(GetUserRequest(userId, identityProviderId = context.identityProviderId))
        .map(_.user.get)

    for {
      // create a normal users
      (alice, _) <- createUserByAdmin(
        testId + "-alice",
        identityProviderId = context.identityProviderId,
      )

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

  it should "deny calls if user is created already within another IDP" taggedAs adminSecurityAsset
    .setAttack(
      attackPermissionDenied(threat = "Present an existing userId but foreign Identity Provider")
    ) in {
    expectPermissionDenied {
      val userId = "fresh-user-" + UUID.randomUUID().toString
      for {
        response1 <- createConfig(canReadAsAdminStandardJWT)
        response2 <- createConfig(canReadAsAdminStandardJWT)

        _ <- createFreshUser(
          userId,
          canReadAsAdmin.token,
          toIdentityProviderId(response1),
          Seq.empty,
        )

        _ <- stub(UserManagementServiceGrpc.stub(channel), canReadAsAdmin.token)
          .getUser(GetUserRequest(userId, identityProviderId = toIdentityProviderId(response2)))
          .map(_.user.get)

      } yield ()
    }
  }
}
