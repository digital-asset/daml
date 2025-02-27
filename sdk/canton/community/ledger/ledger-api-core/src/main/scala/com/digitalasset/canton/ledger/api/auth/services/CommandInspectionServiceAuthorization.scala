// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.auth.services

import com.daml.ledger.api.v2.admin.command_inspection_service.CommandInspectionServiceGrpc.CommandInspectionService
import com.daml.ledger.api.v2.admin.command_inspection_service.{
  CommandInspectionServiceGrpc,
  GetCommandStatusRequest,
  GetCommandStatusResponse,
}
import com.digitalasset.canton.auth.{Authorizer, RequiredClaim}
import com.digitalasset.canton.ledger.api.ProxyCloseable
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class CommandInspectionServiceAuthorization(
    protected val service: CommandInspectionService with AutoCloseable,
    private val authorizer: Authorizer,
)(implicit executionContext: ExecutionContext)
    extends CommandInspectionService
    with ProxyCloseable
    with GrpcApiService {

  override def bindService(): ServerServiceDefinition =
    CommandInspectionServiceGrpc.bindService(this, executionContext)

  override def getCommandStatus(
      request: GetCommandStatusRequest
  ): Future[GetCommandStatusResponse] =
    authorizer.rpc(service.getCommandStatus)(RequiredClaim.Admin())(request)
}
