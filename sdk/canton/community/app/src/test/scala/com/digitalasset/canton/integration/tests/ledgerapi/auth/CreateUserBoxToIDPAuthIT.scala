// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service as uproto
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer

import scala.concurrent.{ExecutionContext, Future}

final class CreateUserBoxToIDPAuthIT extends IDPBoxingServiceCallOutTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String =
    "UserManagementService#CreateUser(<grant-rights-to-IDP-parties>)"

  override protected def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] = ???

  override protected def boxedCall(
      userId: String,
      serviceCallContext: ServiceCallContext,
      rights: Vector[uproto.Right],
  )(implicit ec: ExecutionContext): Future[Any] = createUser(userId, serviceCallContext, rights)

}
