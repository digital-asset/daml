// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service.DeleteUserRequest
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.ApiUserManagementServiceSuppressionRule

import java.util.UUID
import scala.concurrent.Future

final class DeleteUserAuthIT extends AdminOrIDPAdminServiceCallAuthTests with UserManagementAuth {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "UserManagementService#DeleteUser"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    for {
      response <- createFreshUser(context.token, context.identityProviderId)
      userId = response.user.getOrElse(sys.error("Could not create a fresh user")).id
      _ <- stub(context.token).deleteUser(
        DeleteUserRequest(userId = userId, identityProviderId = context.identityProviderId)
      )
    } yield ()
  }

  serviceCallName should {
    "deny calls if user is created already within another IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat = "Present an existing userId but foreign Identity Provider")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule) {
        expectPermissionDenied {
          val userId = "fresh-user-" + UUID.randomUUID().toString
          for {
            idpConfig1 <- createConfig(canBeAnAdmin)
            idpConfig2 <- createConfig(canBeAnAdmin)
            _ <- createFreshUser(
              userId,
              canBeAnAdmin.token,
              idpConfig1.identityProviderId,
              Seq.empty,
            )
            _ <- stub(canBeAnAdmin.token).deleteUser(
              DeleteUserRequest(
                userId = userId,
                identityProviderId = idpConfig2.identityProviderId,
              )
            )
          } yield ()
        }
      }
    }
  }
}
