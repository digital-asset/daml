// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse,
}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.{ProxyCloseable, ValidationLogger}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

class ActiveContractsServiceValidation(
    protected val service: ActiveContractsService with AutoCloseable,
    val ledgerId: LedgerId,
)(implicit executionContext: ExecutionContext)
    extends ActiveContractsService
    with ProxyCloseable
    with GrpcApiService
    with FieldValidations {

  protected implicit val logger: Logger = LoggerFactory.getLogger(service.getClass)

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse],
  ): Unit = {
    matchLedgerId(ledgerId)(LedgerId(request.ledgerId))
      .fold(
        t => responseObserver.onError(ValidationLogger.logFailure(request, t)),
        _ => service.getActiveContracts(request, responseObserver),
      )
  }
  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, executionContext)
}
