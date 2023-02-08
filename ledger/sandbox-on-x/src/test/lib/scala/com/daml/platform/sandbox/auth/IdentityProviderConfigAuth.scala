// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.identity_provider_config_service.{
  CreateIdentityProviderConfigRequest,
  CreateIdentityProviderConfigResponse,
  IdentityProviderConfig,
  IdentityProviderConfigServiceGrpc,
}
import com.daml.platform.sandbox.TestJwtVerifierLoader

import java.util.UUID
import scala.concurrent.Future

trait IdentityProviderConfigAuth {

  this: ServiceCallAuthTests =>

  def idpStub(
      context: ServiceCallContext
  ): IdentityProviderConfigServiceGrpc.IdentityProviderConfigServiceStub =
    stub(IdentityProviderConfigServiceGrpc.stub(channel), context.token)

  def createConfig(context: ServiceCallContext): Future[CreateIdentityProviderConfigResponse] = {
    val suffix = UUID.randomUUID().toString
    val idpId = "idp-id-" + suffix
    val issuer = "issuer-" + suffix
    val config =
      IdentityProviderConfig(
        identityProviderId = idpId,
        isDeactivated = false,
        issuer = issuer,
        jwksUrl =
          TestJwtVerifierLoader.jwksUrl1.value, // token must be signed with `TestJwtVerifierLoader.secret1`
      )
    createConfig(context, config)
  }

  def createConfig(
      context: ServiceCallContext,
      config: IdentityProviderConfig,
  ): Future[CreateIdentityProviderConfigResponse] =
    idpStub(context).createIdentityProviderConfig(CreateIdentityProviderConfigRequest(Some(config)))

}
