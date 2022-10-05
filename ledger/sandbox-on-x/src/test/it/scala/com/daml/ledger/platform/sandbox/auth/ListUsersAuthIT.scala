// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service.ListUsersRequest

import scala.concurrent.Future

final class ListUsersAuthIT extends AdminServiceCallAuthTests with UserManagementAuth {

  override def serviceCallName: String = "UserManagementService#ListUsers"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(token).listUsers(ListUsersRequest())

}
