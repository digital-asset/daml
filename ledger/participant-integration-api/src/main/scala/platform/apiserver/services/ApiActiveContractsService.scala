// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{IndexActiveContractsService => ACSBackend}
import com.daml.dec.DirectExecutionContext
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.daml.ledger.api.v1.active_contracts_service._
import com.daml.ledger.api.validation.TransactionFilterValidator
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.platform.api.grpc.GrpcApiService
import com.daml.platform.server.api.validation.ActiveContractsServiceValidation
import io.grpc.{BindableService, ServerServiceDefinition}

private[apiserver] final class ApiActiveContractsService private (
    backend: ACSBackend,
)(
    implicit protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory,
    loggingContext: LoggingContext,
) extends ActiveContractsServiceAkkaGrpc
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  override protected def getActiveContractsSource(
      request: GetActiveContractsRequest,
  ): Source[GetActiveContractsResponse, NotUsed] =
    withEnrichedLoggingContext(logging.filters(request.getFilter.filtersByParty)) {
      implicit loggingContext: LoggingContext =>
        logger.trace("Serving an Active Contracts request...")
        TransactionFilterValidator
          .validate(request.getFilter)
          .fold(Source.failed, backend.getActiveContracts(_, request.verbose))
          .via(logger.logErrorsOnStream)
    }

  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)
}

private[apiserver] object ApiActiveContractsService {

  def create(ledgerId: LedgerId, backend: ACSBackend)(
      implicit mat: Materializer,
      esf: ExecutionSequencerFactory,
      loggingContext: LoggingContext,
  ): ActiveContractsService with GrpcApiService =
    new ActiveContractsServiceValidation(new ApiActiveContractsService(backend), ledgerId)
    with BindableService {
      override def bindService(): ServerServiceDefinition =
        ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)
    }
}
