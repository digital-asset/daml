// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.{user_management_service => ums}
import com.daml.ledger.api.v1.admin.user_management_service.GrantUserRightsRequest

import scala.concurrent.Future

final class GrantUserRightsAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with UserManagementAuth
    with GrantPermissionTest {

  override def serviceCallName: String = "UserManagementService#GrantUserRights"

  def serviceCallWithGrantPermission(
      context: ServiceCallContext,
      permission: ums.Right,
  ): Future[Any] =
    for {
      response <- createFreshUser(context.token, context.identityProviderId)
      userId = response.user.getOrElse(sys.error("Could not load create a fresh user")).id
      _ <- stub(context.token).grantUserRights(
        GrantUserRightsRequest(
          userId = userId,
          rights = scala.Seq(permission),
          identityProviderId = context.identityProviderId,
        )
      )
    } yield {}

}
