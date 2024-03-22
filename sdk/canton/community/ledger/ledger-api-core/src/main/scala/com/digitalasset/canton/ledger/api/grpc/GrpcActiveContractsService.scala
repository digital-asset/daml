// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.active_contracts_service.{
  ActiveContractsServiceGrpc,
  GetActiveContractsRequest,
  GetActiveContractsResponse,
}
import com.daml.logging.LoggingContext
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.{LedgerId, optionalLedgerId}
import com.digitalasset.canton.ledger.api.validation.FieldValidator
import com.digitalasset.canton.ledger.api.{ProxyCloseable, ValidationLogger}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext

class GrpcActiveContractsService(
    protected val service: ActiveContractsService with AutoCloseable,
    val ledgerId: LedgerId,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ActiveContractsService
    with ProxyCloseable
    with GrpcApiService
    with NamedLogging {

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse],
  ): Unit = {
    implicit val loggingContext = LoggingContextWithTrace(telemetry)(LoggingContext.empty)

    FieldValidator
      .matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      .fold(
        t => responseObserver.onError(ValidationLogger.logFailureWithTrace(logger, request, t)),
        _ => service.getActiveContracts(request, responseObserver),
      )
  }
  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, executionContext)
}
