// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.identity_provider_config_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

class IdentityProviderConfigServiceAuthorization(
    protected val service: IdentityProviderConfigServiceGrpc.IdentityProviderConfigService
      with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends IdentityProviderConfigServiceGrpc.IdentityProviderConfigService
    with ProxyCloseable
    with GrpcApiService {

  override def createIdentityProviderConfig(
      request: CreateIdentityProviderConfigRequest
  ): Future[CreateIdentityProviderConfigResponse] =
    authorizer.requireAdminClaims(service.createIdentityProviderConfig)(request)

  override def getIdentityProviderConfig(
      request: GetIdentityProviderConfigRequest
  ): Future[GetIdentityProviderConfigResponse] =
    authorizer.requireAdminClaims(service.getIdentityProviderConfig)(request)

  override def updateIdentityProviderConfig(
      request: UpdateIdentityProviderConfigRequest
  ): Future[UpdateIdentityProviderConfigResponse] =
    authorizer.requireAdminClaims(service.updateIdentityProviderConfig)(request)

  override def listIdentityProviderConfigs(
      request: ListIdentityProviderConfigsRequest
  ): Future[ListIdentityProviderConfigsResponse] =
    authorizer.requireAdminClaims(service.listIdentityProviderConfigs)(request)

  override def deleteIdentityProviderConfig(
      request: DeleteIdentityProviderConfigRequest
  ): Future[DeleteIdentityProviderConfigResponse] =
    authorizer.requireAdminClaims(service.deleteIdentityProviderConfig)(request)

  override def bindService(): ServerServiceDefinition =
    IdentityProviderConfigServiceGrpc.bindService(this, executionContext)
}
