// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service.{
  GrantUserRightsRequest,
  RevokeUserRightsRequest,
}
import com.daml.ledger.api.v1.admin.{user_management_service => ums}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._

import java.util.UUID
import scala.concurrent.Future

final class RevokeUserRightsAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with UserManagementAuth
    with GrantPermissionTest {

  override def serviceCallName: String = "UserManagementService#RevokeUserRights"

  def serviceCallWithGrantPermission(
      context: ServiceCallContext,
      permission: ums.Right,
  ): Future[Any] =
    for {
      response <- createFreshUser(context.token, context.identityProviderId)
      userId = response.user.getOrElse(sys.error("Could not load create a fresh user")).id
      _ <- stub(canReadAsAdminStandardJWT.token).grantUserRights(
        GrantUserRightsRequest(
          userId = userId,
          rights = scala.Seq(permission),
          identityProviderId = context.identityProviderId,
        )
      )
      _ <- stub(context.token).revokeUserRights(
        RevokeUserRightsRequest(
          userId = userId,
          rights = scala.Seq(permission),
          identityProviderId = context.identityProviderId,
        )
      )
    } yield {}

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

        _ <- stub(canReadAsAdminStandardJWT.token).grantUserRights(
          GrantUserRightsRequest(
            userId = userId,
            rights = scala.Seq(idpAdminPermission),
            identityProviderId = toIdentityProviderId(response1),
          )
        )
        _ <- stub(canReadAsAdminStandardJWT.token).revokeUserRights(
          RevokeUserRightsRequest(
            userId = userId,
            rights = scala.Seq(idpAdminPermission),
            identityProviderId = toIdentityProviderId(response2),
          )
        )

      } yield ()
    }
  }

}
