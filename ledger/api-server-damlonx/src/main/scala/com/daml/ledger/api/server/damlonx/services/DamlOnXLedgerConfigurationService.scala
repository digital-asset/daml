// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import java.time.Duration

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v1.IndexService
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.digitalasset.ledger.api.v1.ledger_configuration_service._
import com.digitalasset.platform.api.grpc.{GrpcApiService, GrpcApiUtil}
import com.digitalasset.platform.common.util.DirectExecutionContext
import io.grpc.{BindableService, ServerServiceDefinition}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

class DamlOnXLedgerConfigurationService private (indexService: IndexService)(
    implicit protected val esf: ExecutionSequencerFactory,
    implicit val ec: ExecutionContext,
    protected val mat: Materializer)
    extends LedgerConfigurationServiceAkkaGrpc
    with GrpcApiService
    with DamlOnXServiceUtils {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  override protected def getLedgerConfigurationSource(
      request: GetLedgerConfigurationRequest): Source[GetLedgerConfigurationResponse, NotUsed] = {

    Source
      .fromFuture(
        consumeAsyncResult(indexService
          .getLedgerConfiguration(Ref.SimpleString.assertFromString(request.ledgerId))))
      .map { config =>
        // FIXME(JM): Configuration type not yet defined.
        //GetLedgerConfigurationResponse(Some(config))
        GetLedgerConfigurationResponse(
          Some(
            LedgerConfiguration(
              Some(GrpcApiUtil.durationToProto(Duration.ofSeconds(1L))),
              Some(GrpcApiUtil.durationToProto(Duration.ofSeconds(30L)))),
          )
        )
      }
  }

  override def bindService(): ServerServiceDefinition =
    LedgerConfigurationServiceGrpc.bindService(this, DirectExecutionContext)
}

object DamlOnXLedgerConfigurationService {
  def apply(indexService: IndexService)(
      implicit ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer)
    : LedgerConfigurationService with BindableService with LedgerConfigurationServiceLogging =
    new DamlOnXLedgerConfigurationService(indexService) with LedgerConfigurationServiceLogging
}
