// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfigurationServiceGrpc,
}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.{ProxyCloseable, ValidationLogger}
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

class LedgerConfigurationServiceValidation(
    protected val service: LedgerConfigurationService with GrpcApiService,
    protected val ledgerId: LedgerId,
)(implicit executionContext: ExecutionContext)
    extends LedgerConfigurationService
    with ProxyCloseable
    with GrpcApiService
    with FieldValidations {

  protected implicit val logger: Logger = LoggerFactory.getLogger(service.getClass)

  override def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse],
  ): Unit =
    matchLedgerId(ledgerId)(LedgerId(request.ledgerId)).fold(
      t => responseObserver.onError(ValidationLogger.logFailure(request, t)),
      _ => service.getLedgerConfiguration(request, responseObserver),
    )

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, executionContext)
}
