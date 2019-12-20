// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.auth.services

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.auth.Authorizer
import com.digitalasset.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc.ConfigManagementService
import com.digitalasset.ledger.api.v1.admin.config_management_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.ProxyCloseable
import io.grpc.ServerServiceDefinition

import scala.concurrent.Future

final class ConfigManagementServiceAuthorization(
    protected val service: ConfigManagementService with AutoCloseable,
    private val authorizer: Authorizer)
    extends ConfigManagementService
    with ProxyCloseable
    with GrpcApiService {

  override def getTimeModel(request: GetTimeModelRequest): Future[GetTimeModelResponse] =
    authorizer.requireAdminClaims(service.getTimeModel)(request)

  override def setTimeModel(request: SetTimeModelRequest): Future[SetTimeModelResponse] =
    authorizer.requireAdminClaims(service.setTimeModel)(request)

  override def bindService(): ServerServiceDefinition =
    ConfigManagementServiceGrpc.bindService(this, DirectExecutionContext)

  override def close(): Unit = service.close()
}
