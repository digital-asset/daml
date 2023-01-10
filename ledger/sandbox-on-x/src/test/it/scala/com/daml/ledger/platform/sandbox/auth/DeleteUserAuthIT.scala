// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service.DeleteUserRequest

import scala.concurrent.Future

final class DeleteUserAuthIT extends AdminServiceCallAuthTests with UserManagementAuth {

  override def serviceCallName: String = "UserManagementService#DeleteUser"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    for {
      response <- createFreshUser(context.token, context.identityProviderId)
      userId = response.user.getOrElse(sys.error("Could not load create a fresh user")).id
      _ <- stub(context.token).deleteUser(
        DeleteUserRequest(userId = userId, identityProviderId = context.identityProviderId)
      )
    } yield ()

}
