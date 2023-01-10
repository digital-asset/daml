// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import akka.stream.Materializer
import com.daml.api.util.DurationConversion._
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.grpc.adapter.server.akka.StreamingServiceLifecycleManagement
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_configuration_service._
import com.daml.ledger.participant.state.index.v2.IndexConfigurationService
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.LedgerConfigurationServiceValidation
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext

private[apiserver] final class ApiLedgerConfigurationService private (
    configurationService: IndexConfigurationService
)(implicit
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends LedgerConfigurationServiceGrpc.LedgerConfigurationService
    with StreamingServiceLifecycleManagement
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)
  protected implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse],
  ): Unit = registerStream(responseObserver) {
    logger.info(s"Received request for configuration subscription: $request")
    configurationService
      .getLedgerConfiguration()
      .map(configuration =>
        GetLedgerConfigurationResponse(
          Some(
            LedgerConfiguration(
              Some(toProto(configuration.maxDeduplicationDuration))
            )
          )
        )
      )
      .via(logger.logErrorsOnStream)
  }

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, executionContext)
}

private[apiserver] object ApiLedgerConfigurationService {
  def create(
      ledgerId: LedgerId,
      configurationService: IndexConfigurationService,
  )(implicit
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): LedgerConfigurationServiceGrpc.LedgerConfigurationService with GrpcApiService = {
    new LedgerConfigurationServiceValidation(
      service = new ApiLedgerConfigurationService(configurationService),
      ledgerId = ledgerId,
    ) with BindableService {
      override def bindService(): ServerServiceDefinition =
        LedgerConfigurationServiceGrpc.bindService(this, executionContext)
    }
  }
}
