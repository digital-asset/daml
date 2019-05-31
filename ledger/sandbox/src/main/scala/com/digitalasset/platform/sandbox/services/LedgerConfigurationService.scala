// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.IndexConfigurationService
import com.digitalasset.api.util.DurationConversion._
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.ledger_configuration_service._
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.api.validation.LedgerConfigurationServiceValidation
import io.grpc.{BindableService, ServerServiceDefinition}

import scala.concurrent.ExecutionContext

class LedgerConfigurationService private (configurationService: IndexConfigurationService)(
    implicit protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer)
    extends LedgerConfigurationServiceAkkaGrpc
    with GrpcApiService {

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

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, DirectExecutionContext)
}

object LedgerConfigurationService {
  def createApiService(ledgerId: LedgerId, configurationService: IndexConfigurationService)(
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
