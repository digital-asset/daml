// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.user_management_service.{
  CreateUserRequest,
  CreateUserResponse,
  User,
  UserManagementServiceGrpc,
}

import java.util.UUID
import scala.concurrent.Future

trait UserManagementAuth {

  this: ServiceCallAuthTests =>

  def stub(token: Option[String]): UserManagementServiceGrpc.UserManagementServiceStub =
    stub(UserManagementServiceGrpc.stub(channel), token)

  def createFreshUser(token: Option[String]): Future[CreateUserResponse] = {
    val userId = "fresh-user-" + UUID.randomUUID().toString
    val req = CreateUserRequest(Some(User(userId)))
    stub(token).createUser(req)
  }

}
