// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import scala.concurrent.Future
import com.daml.ledger.api.v1.admin.{user_management_service => ums}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.util.UUID

final class CreateUserAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with UserManagementAuth
    with GrantPermissionTest {

  override def serviceCallName: String = "UserManagementService#CreateUser"

  def serviceCallWithGrantPermission(
      context: ServiceCallContext,
      permission: ums.Right,
  ): Future[Any] =
    createFreshUser(context.token, context.identityProviderId, scala.Seq(permission))

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
        _ <- createFreshUser(
          userId,
          canReadAsAdmin.token,
          identityProviderId(response2),
          Seq.empty,
        )

      } yield ()
    }
  }
}
