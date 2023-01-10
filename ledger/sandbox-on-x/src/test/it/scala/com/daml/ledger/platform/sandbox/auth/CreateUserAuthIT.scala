// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import scala.concurrent.Future

final class CreateUserAuthIT extends AdminServiceCallAuthTests with UserManagementAuth {

  override def serviceCallName: String = "UserManagementService#CreateUser"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    createFreshUser(context.token, context.identityProviderId)
}
