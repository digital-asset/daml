// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v1.ConfigurationService
import com.digitalasset.api.util.DurationConversion._
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  GetLedgerConfigurationResponse,
  LedgerConfiguration,
  LedgerConfigurationServiceAkkaGrpc,
  LedgerConfigurationServiceGrpc,
  LedgerConfigurationServiceLogging
}
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.server.api.validation.LedgerConfigurationServiceValidation
import com.digitalasset.platform.common.util.DirectExecutionContext
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.{ExecutionContext, Promise}

class LedgerConfigurationService private (configurationService: ConfigurationService)(
    implicit protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer)
    extends LedgerConfigurationServiceAkkaGrpc
    with GrpcApiService {

  override protected def getLedgerConfigurationSource(
      request: GetLedgerConfigurationRequest): Source[GetLedgerConfigurationResponse, NotUsed] =
    Source
      .fromFuture(configurationService.getLedgerConfiguration())
      .map(
        configuration =>
          GetLedgerConfigurationResponse(
            Some(
              LedgerConfiguration(
                Some(toProto(configuration.timeModel.minTtl)),
                Some(toProto(configuration.timeModel.maxTtl))
              ))))
      .concat(Source.fromFuture(Promise[GetLedgerConfigurationResponse]().future)) // we should keep the stream open!

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, DirectExecutionContext)
}

object LedgerConfigurationService {
  def createApiService(configurationService: ConfigurationService, ledgerId: String)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer)
    : GrpcApiService with BindableService with LedgerConfigurationServiceLogging =
    new LedgerConfigurationServiceValidation(
      new LedgerConfigurationService(configurationService),
      ledgerId) with BindableService with LedgerConfigurationServiceLogging {
      override def bindService(): ServerServiceDefinition =
        LedgerConfigurationServiceGrpc.bindService(this, DirectExecutionContext)
    }
}
