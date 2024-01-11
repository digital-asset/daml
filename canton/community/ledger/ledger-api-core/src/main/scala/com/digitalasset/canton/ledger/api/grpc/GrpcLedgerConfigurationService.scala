// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.grpc

import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfigurationServiceGrpc,
}
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.{LedgerId, optionalLedgerId}
import com.digitalasset.canton.ledger.api.validation.FieldValidator
import com.digitalasset.canton.ledger.api.{ProxyCloseable, ValidationLogger}
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext

class GrpcLedgerConfigurationService(
    protected val service: LedgerConfigurationService with GrpcApiService,
    protected val ledgerId: LedgerId,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends LedgerConfigurationService
    with ProxyCloseable
    with GrpcApiService
    with NamedLogging {

  override def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse],
  ): Unit = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)
    FieldValidator
      .matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      .fold(
        t => responseObserver.onError(ValidationLogger.logFailureWithTrace(logger, request, t)),
        _ => service.getLedgerConfiguration(request, responseObserver),
      )
  }

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, executionContext)
}
