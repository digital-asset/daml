// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.identity_provider_config_service.ListIdentityProviderConfigsRequest

import scala.concurrent.Future

final class ListIdentityProviderConfigsAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {

  override def serviceCallName: String =
    "IdentityProviderConfigService#ListIdentityProviderConfigs"

  override def serviceCall(context: ServiceCallContext): Future[Any] =
    idpStub(context).listIdentityProviderConfigs(ListIdentityProviderConfigsRequest())

}
