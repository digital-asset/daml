// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.auth.services

import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.auth.Authorizer
import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc.ConfigManagementService
import com.daml.ledger.api.v1.admin.config_management_service._
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ProxyCloseable
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
