// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
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

import scala.concurrent.ExecutionContext

class LedgerConfigurationServiceImpl private (ledgerConfiguration: LedgerConfiguration)(
    implicit protected val esf: ExecutionSequencerFactory,
    protected val mat: Materializer)
    extends LedgerConfigurationServiceAkkaGrpc
    with GrpcApiService {

  override protected def getLedgerConfigurationSource(
      request: GetLedgerConfigurationRequest): Source[GetLedgerConfigurationResponse, NotUsed] = {
    Source.single(GetLedgerConfigurationResponse(Some(ledgerConfiguration)))
  }

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, DirectExecutionContext)
}

object LedgerConfigurationServiceImpl {
  def apply(ledgerConfiguration: LedgerConfiguration, ledgerId: String)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer)
    : LedgerConfigurationService with BindableService with LedgerConfigurationServiceLogging =
    new LedgerConfigurationServiceValidation(
      new LedgerConfigurationServiceImpl(ledgerConfiguration),
      ledgerId) with BindableService with LedgerConfigurationServiceLogging {
      override def bindService(): ServerServiceDefinition =
        LedgerConfigurationServiceGrpc.bindService(this, DirectExecutionContext)
    }
}
