// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.api.util.DurationConversion.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v1.ledger_configuration_service.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.domain.LedgerId
import com.digitalasset.canton.ledger.api.grpc.{
  GrpcApiService,
  GrpcLedgerConfigurationService,
  StreamingServiceLifecycleManagement,
}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexConfigurationService
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import io.grpc.stub.StreamObserver
import io.grpc.{BindableService, ServerServiceDefinition}
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext

private[apiserver] final class ApiLedgerConfigurationService private (
    configurationService: IndexConfigurationService,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    esf: ExecutionSequencerFactory,
    mat: Materializer,
    executionContext: ExecutionContext,
) extends LedgerConfigurationServiceGrpc.LedgerConfigurationService
    with StreamingServiceLifecycleManagement
    with GrpcApiService
    with NamedLogging {

  def getLedgerConfiguration(
      request: GetLedgerConfigurationRequest,
      responseObserver: StreamObserver[GetLedgerConfigurationResponse],
  ): Unit = {
    implicit val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    registerStream(responseObserver) {
      logger.info(s"Received request for configuration subscription: $request.")
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
  }

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, executionContext)
}

private[apiserver] object ApiLedgerConfigurationService {
  def create(
      ledgerId: LedgerId,
      configurationService: IndexConfigurationService,
      telemetry: Telemetry,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      esf: ExecutionSequencerFactory,
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): LedgerConfigurationServiceGrpc.LedgerConfigurationService with GrpcApiService = {
    new GrpcLedgerConfigurationService(
      service = new ApiLedgerConfigurationService(configurationService, telemetry, loggerFactory),
      ledgerId = ledgerId,
      telemetry = telemetry,
      loggerFactory = loggerFactory,
    ) with BindableService {
      override def bindService(): ServerServiceDefinition =
        LedgerConfigurationServiceGrpc.bindService(this, executionContext)
    }
  }
}
