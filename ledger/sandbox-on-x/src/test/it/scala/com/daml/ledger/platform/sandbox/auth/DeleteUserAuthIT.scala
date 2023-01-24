// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service.DeleteUserRequest

import java.util.UUID
import scala.concurrent.Future
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

final class DeleteUserAuthIT extends AdminOrIDPAdminServiceCallAuthTests with UserManagementAuth {

  override def serviceCallName: String = "UserManagementService#DeleteUser"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    for {
      response <- createFreshUser(context.token, context.identityProviderId)
      userId = response.user.getOrElse(sys.error("Could not load create a fresh user")).id
      _ <- stub(context.token).deleteUser(
        DeleteUserRequest(userId = userId, identityProviderId = context.identityProviderId)
      )
    } yield ()

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
          identityProviderId(response1),
          Seq.empty,
        )

        _ <- stub(canReadAsAdmin.token).deleteUser(
          DeleteUserRequest(userId = userId, identityProviderId = identityProviderId(response2))
        )

      } yield ()
    }
  }
}
