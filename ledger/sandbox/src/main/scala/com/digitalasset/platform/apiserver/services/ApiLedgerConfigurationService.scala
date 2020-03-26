// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.IndexConfigurationService
import com.digitalasset.api.util.DurationConversion._
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.ledger_configuration_service._
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.validation.LedgerConfigurationServiceValidation
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext

final class ApiLedgerConfigurationService private (configurationService: IndexConfigurationService)(
    implicit protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer,
    logCtx: LoggingContext)
    extends LedgerConfigurationServiceAkkaGrpc
    with GrpcApiService {

  private val logger = ContextualizedLogger.get(this.getClass)

  override protected def getLedgerConfigurationSource(
      request: GetLedgerConfigurationRequest): Source[GetLedgerConfigurationResponse, NotUsed] =
    configurationService
      .getLedgerConfiguration()
      .map(
        configuration =>
          GetLedgerConfigurationResponse(
            Some(
              LedgerConfiguration(
                Some(toProto(configuration.minTTL)),
                Some(toProto(configuration.maxTTL))
              ))))
      .via(logger.logErrorsOnStream)

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, DirectExecutionContext)
}

object ApiLedgerConfigurationService {
  def create(ledgerId: LedgerId, configurationService: IndexConfigurationService)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
      logCtx: LoggingContext)
    : LedgerConfigurationServiceGrpc.LedgerConfigurationService with GrpcApiService =
    new LedgerConfigurationServiceValidation(
      new ApiLedgerConfigurationService(configurationService),
      ledgerId) with BindableService {
      override def bindService(): ServerServiceDefinition =
        LedgerConfigurationServiceGrpc.bindService(this, DirectExecutionContext)
    }
}
