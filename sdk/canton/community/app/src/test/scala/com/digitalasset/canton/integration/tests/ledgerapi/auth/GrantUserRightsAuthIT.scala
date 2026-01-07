// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as ums
import com.daml.ledger.api.v2.admin.user_management_service.GrantUserRightsRequest
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.ApiUserManagementServiceSuppressionRule

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

final class GrantUserRightsAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with UserManagementAuth
    with GrantPermissionTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "UserManagementService#GrantUserRights"

  def serviceCallWithGrantPermission(
      context: ServiceCallContext,
      permission: ums.Right,
  )(implicit ec: ExecutionContext): Future[Any] =
    for {
      response <- createFreshUser(context.token, context.identityProviderId)
      userId = response.user.getOrElse(sys.error("Could not create a fresh user")).id
      _ <- stub(context.token).grantUserRights(
        GrantUserRightsRequest(
          userId = userId,
          rights = scala.Seq(permission),
          identityProviderId = context.identityProviderId,
        )
      )
    } yield {}

  serviceCallName should {
    "deny granting IDP Admin rights to a user within another IDP" taggedAs adminSecurityAsset
      .setAttack(
        attackPermissionDenied(threat = "Grant IDP Admin rights in a foreign IDP")
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
            _ <- stub(canBeAnAdmin.token).grantUserRights(
              GrantUserRightsRequest(
                userId = userId,
                rights = scala.Seq(idpAdminPermission),
                identityProviderId = idpConfig2.identityProviderId,
              )
            )
          } yield ()
        }
      }
    }
  }
}
