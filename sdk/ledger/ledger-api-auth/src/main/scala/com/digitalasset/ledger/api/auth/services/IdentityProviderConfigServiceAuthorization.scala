// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.admin.identity_provider_config_service._
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
