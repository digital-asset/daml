// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.api.util.DurationConversion._
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.ledger_configuration_service._
import com.daml.ledger.participant.state.index.v2.IndexConfigurationService
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.LedgerConfigurationServiceValidation
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext

private[apiserver] final class ApiLedgerConfigurationService private (
    configurationService: IndexConfigurationService
)(implicit
    protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends LedgerConfigurationServiceAkkaGrpc
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  override protected def getLedgerConfigurationSource(
      request: GetLedgerConfigurationRequest
  ): Source[GetLedgerConfigurationResponse, NotUsed] = {
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
      .watchTermination()(logger.logTermination)
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
