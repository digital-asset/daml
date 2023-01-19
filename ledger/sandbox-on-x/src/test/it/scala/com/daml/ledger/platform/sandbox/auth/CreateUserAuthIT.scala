// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import scala.concurrent.Future
import com.daml.ledger.api.v1.admin.{user_management_service => ums}

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
}
