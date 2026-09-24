// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as ums
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.{
  ApiUserManagementServiceSuppressionRule,
  IDPAndJWTSuppressionRule,
}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

final class CreateUserAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with UserManagementAuth
    with GrantPermissionTest {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "UserManagementService#CreateUser"

  def serviceCallWithGrantPermission(
      context: ServiceCallContext,
      permission: ums.Right,
  )(implicit ec: ExecutionContext): Future[Any] =
    createFreshUser(context.token, context.identityProviderId, scala.Seq(permission))

  serviceCallName should {
    "deny duplicate if user is created already within another IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat = "Duplicate userId creation in a different IDP")
      ) in { implicit env =>
      import env.*
      loggerFactory.suppress(ApiUserManagementServiceSuppressionRule || IDPAndJWTSuppressionRule) {
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
            _ <- createFreshUser(
              userId,
              canBeAnAdmin.token,
              idpConfig2.identityProviderId,
              Seq.empty,
            )
          } yield ()
        }
      }
    }
  }
}
