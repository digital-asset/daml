// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service.UpdateUserRequest
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.ApiUserManagementServiceSuppressionRule
import com.google.protobuf.field_mask.FieldMask

import java.util.UUID
import scala.concurrent.Future

final class UpdateUserAuthIT extends AdminOrIDPAdminServiceCallAuthTests with UserManagementAuth {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "UserManagementService#UpdateUser"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    for {
      response <- createFreshUser(context.token, context.identityProviderId)
      _ <- stub(context.token).updateUser(
        UpdateUserRequest(
          user = response.user,
          updateMask = Some(FieldMask(scala.Seq("is_deactivated"))),
        )
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
            createUserResponse <- createFreshUser(
              userId,
              canBeAnAdmin.token,
              idpConfig1.identityProviderId,
              Seq.empty,
            )
            _ <- stub(canBeAnAdmin.token).updateUser(
              UpdateUserRequest(
                user = createUserResponse.user.map(user =>
                  user.copy(identityProviderId = idpConfig2.identityProviderId)
                ),
                updateMask = Some(FieldMask(scala.Seq("is_deactivated"))),
              )
            )
          } yield ()
        }
      }
    }
  }

}
