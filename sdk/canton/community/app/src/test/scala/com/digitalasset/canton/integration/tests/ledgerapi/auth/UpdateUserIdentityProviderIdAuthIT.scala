// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.user_management_service.{
  UpdateUserIdentityProviderIdRequest,
  UserManagementServiceGrpc,
}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}

import scala.concurrent.Future

final class UpdateUserIdentityProviderIdAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth
    with UserManagementAuth {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def serviceCallName: String = "UserManagementService#UpdateUserIdentityProviderId"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    import env.*
    for {
      idpConfig <- createConfig(canBeAnAdmin)
      idpId = idpConfig.identityProviderId
      createUserResp <- createFreshUser(canBeAnAdmin.token, identityProviderId = "")
      userId = createUserResp.user.value.id
      result <- stub(UserManagementServiceGrpc.stub(channel), context.token)
        .updateUserIdentityProviderId(
          UpdateUserIdentityProviderIdRequest(
            userId = userId,
            sourceIdentityProviderId = "",
            targetIdentityProviderId = idpId,
          )
        )
      // cleanup the idp configuration we created in order to prevent exceeding the max number of possible idp configs
      _ <- parkUsers(canBeAnAdmin, List(userId), idpId)
      _ <- deleteConfig(canBeAnAdmin, identityProviderId = idpId)
    } yield result
  }

}
