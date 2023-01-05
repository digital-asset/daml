// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.identity_provider_config_service.DeleteIdentityProviderConfigRequest

import scala.concurrent.Future

final class DeleteIdentityProviderConfigsAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {

  override def serviceCallName: String =
    "IdentityProviderConfigService#ListIdentityProviderConfigs"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    for {
      response <- createConfig(context)
      identityProviderId = response.identityProviderConfig
        .getOrElse(sys.error("Could not load create an identity provider configuration"))
        .identityProviderId
      _ <- idpStub(context).deleteIdentityProviderConfig(
        DeleteIdentityProviderConfigRequest(identityProviderId)
      )
    } yield ()

}
