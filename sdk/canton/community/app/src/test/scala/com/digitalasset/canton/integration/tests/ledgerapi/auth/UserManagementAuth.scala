// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service.*

import java.util.UUID
import scala.concurrent.Future

trait UserManagementAuth {

  this: ServiceCallAuthTests =>

  def stub(token: Option[String]): UserManagementServiceGrpc.UserManagementServiceStub =
    stub(UserManagementServiceGrpc.stub(channel), token)

  def createFreshUser(
      token: Option[String],
      identityProviderId: String,
      rights: scala.Seq[Right] = scala.Seq.empty,
  ): Future[CreateUserResponse] = {
    val userId = "fresh-user-" + UUID.randomUUID().toString
    createFreshUser(userId, token, identityProviderId, rights)
  }

  def createFreshUser(
      userId: String,
      token: Option[String],
      identityProviderId: String,
      rights: scala.Seq[Right],
  ): Future[CreateUserResponse] = {
    val req = CreateUserRequest(
      user = Some(
        User(
          id = userId,
          primaryParty = "",
          isDeactivated = false,
          metadata = None,
          identityProviderId = identityProviderId,
        )
      ),
      rights = rights,
    )
    stub(token).createUser(req)
  }

}
