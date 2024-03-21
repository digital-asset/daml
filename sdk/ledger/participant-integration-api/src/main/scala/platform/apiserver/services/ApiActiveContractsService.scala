// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.active_contracts_service._
import com.daml.ledger.api.validation.TransactionFilterValidator
import com.daml.ledger.participant.state.index.v2.{IndexActiveContractsService => ACSBackend}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.ValidationLogger
import com.daml.platform.server.api.validation.ActiveContractsServiceValidation
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext

private[apiserver] final class ApiActiveContractsService private (
    backend: ACSBackend,
    metrics: Metrics,
)(implicit
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    loggingContext: LoggingContext,
) extends ActiveContractsServiceAkkaGrpc
    with GrpcApiService {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  override protected def getActiveContractsSource(
      request: GetActiveContractsRequest
  ): Source[GetActiveContractsResponse, NotUsed] =
    TransactionFilterValidator
      .validate(request.getFilter)
      .fold(
        t => Source.failed(ValidationLogger.logFailure(request, t)),
        filters =>
          withEnrichedLoggingContext(logging.filters(filters)) { implicit loggingContext =>
            logger.info(s"Received request for active contracts: $request")
            backend.getActiveContracts(filters, request.verbose)
          },
      )
      .via(logger.logErrorsOnStream)
      .via(StreamMetrics.countElements(metrics.daml.lapi.streams.acs))

  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, executionContext)
}

private[apiserver] object ApiActiveContractsService {

  def create(
      ledgerId: LedgerId,
      backend: ACSBackend,
      metrics: Metrics,
  )(implicit
      mat: Materializer,
      esf: ExecutionSequencerFactory,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ActiveContractsService with GrpcApiService = {
    val service = new ApiActiveContractsService(
      backend = backend,
      metrics = metrics,
    )
    new ActiveContractsServiceValidation(
      service = service,
      ledgerId = ledgerId,
    ) with BindableService {
      override def bindService(): ServerServiceDefinition =
        ActiveContractsServiceGrpc.bindService(this, executionContext)
    }
  }
}
