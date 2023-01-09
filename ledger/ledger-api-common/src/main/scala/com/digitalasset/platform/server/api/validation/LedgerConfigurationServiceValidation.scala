// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.ledger.api.domain.{LedgerId, optionalLedgerId}
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfigurationServiceGrpc,
}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.{ProxyCloseable, ValidationLogger}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver

import scala.concurrent.ExecutionContext

class LedgerConfigurationServiceValidation(
    protected val service: LedgerConfigurationService with GrpcApiService,
    protected val ledgerId: LedgerId,
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends LedgerConfigurationService
    with ProxyCloseable
    with GrpcApiService {

  protected implicit val logger: ContextualizedLogger = ContextualizedLogger.get(service.getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse],
  ): Unit =
    FieldValidations
      .matchLedgerId(ledgerId)(optionalLedgerId(request.ledgerId))
      .fold(
        t => responseObserver.onError(ValidationLogger.logFailure(request, t)),
        _ => service.getLedgerConfiguration(request, responseObserver),
      )

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, executionContext)
}
