// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.identity_provider_config_service.ListIdentityProviderConfigsRequest
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer

import scala.concurrent.Future

final class ListIdentityProviderConfigsAuthIT
    extends AdminServiceCallAuthTests
    with IdentityProviderConfigAuth {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String =
    "IdentityProviderConfigService#ListIdentityProviderConfigs"

  override def serviceCall(context: ServiceCallContext)(implicit
      env: TestConsoleEnvironment
  ): Future[Any] =
    idpStub(context).listIdentityProviderConfigs(ListIdentityProviderConfigsRequest())

}
