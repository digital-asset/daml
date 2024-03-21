// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v1.admin.config_management_service.ConfigManagementServiceGrpc.ConfigManagementService
import com.daml.ledger.api.v1.admin.config_management_service.*
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.auth.Authorizer
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class ConfigManagementServiceAuthorization(
    protected val service: ConfigManagementService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends ConfigManagementService
    with ProxyCloseable
    with GrpcApiService {

  override def getTimeModel(request: GetTimeModelRequest): Future[GetTimeModelResponse] =
    authorizer.requireAdminClaims(service.getTimeModel)(request)

  override def setTimeModel(request: SetTimeModelRequest): Future[SetTimeModelResponse] =
    authorizer.requireAdminClaims(service.setTimeModel)(request)

  override def bindService(): ServerServiceDefinition =
    ConfigManagementServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = service.close()
}
