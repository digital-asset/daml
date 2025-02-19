// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.identity_provider_config_service.*
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
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
    authorizer.rpc(service.createIdentityProviderConfig)(RequiredClaim.Admin())(request)

  override def getIdentityProviderConfig(
      request: GetIdentityProviderConfigRequest
  ): Future[GetIdentityProviderConfigResponse] =
    authorizer.rpc(service.getIdentityProviderConfig)(RequiredClaim.Admin())(request)

  override def updateIdentityProviderConfig(
      request: UpdateIdentityProviderConfigRequest
  ): Future[UpdateIdentityProviderConfigResponse] =
    authorizer.rpc(service.updateIdentityProviderConfig)(RequiredClaim.Admin())(request)

  override def listIdentityProviderConfigs(
      request: ListIdentityProviderConfigsRequest
  ): Future[ListIdentityProviderConfigsResponse] =
    authorizer.rpc(service.listIdentityProviderConfigs)(RequiredClaim.Admin())(request)

  override def deleteIdentityProviderConfig(
      request: DeleteIdentityProviderConfigRequest
  ): Future[DeleteIdentityProviderConfigResponse] =
    authorizer.rpc(service.deleteIdentityProviderConfig)(RequiredClaim.Admin())(request)

  override def bindService(): ServerServiceDefinition =
    IdentityProviderConfigServiceGrpc.bindService(this, executionContext)
}
