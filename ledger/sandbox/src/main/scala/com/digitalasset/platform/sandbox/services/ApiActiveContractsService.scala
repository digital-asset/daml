// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{
  ActiveContractSetSnapshot,
  IndexActiveContractsService => ACSBackend
}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.active_contracts_service._
import com.digitalasset.ledger.api.v1.event.CreatedEvent
import com.digitalasset.ledger.api.validation.TransactionFilterValidator
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.digitalasset.platform.server.api.validation.{
  ActiveContractsServiceValidation,
  IdentifierResolver
}
import io.grpc.{BindableService, ServerServiceDefinition}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

import scalaz.syntax.tag._

class ApiActiveContractsService private (
    backend: ACSBackend,
    identifierResolver: IdentifierResolver,
    parallelism: Int = Runtime.getRuntime.availableProcessors)(
    implicit executionContext: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends ActiveContractsServiceAkkaGrpc
    with GrpcApiService {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val txFilterValidator = new TransactionFilterValidator(identifierResolver)

  @SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  override protected def getActiveContractsSource(
      request: GetActiveContractsRequest): Source[GetActiveContractsResponse, NotUsed] = {
    logger.trace("Serving an Active Contracts request...")

    txFilterValidator
      .validate(request.getFilter, "filter")
      .fold(
        Source.failed, { filter =>
          Source
            .fromFuture(backend.getActiveContractSetSnapshot(filter))
            .flatMapConcat {
              case ActiveContractSetSnapshot(offset, acsStream) =>
                acsStream
                  .map {
                    case (wfId, create) =>
                      GetActiveContractsResponse(
                        workflowId = wfId.map(_.unwrap).getOrElse(""),
                        activeContracts = List(
                          CreatedEvent(
                            create.eventId.unwrap,
                            create.contractId.coid,
                            Some(LfEngineToApi.toApiIdentifier(create.templateId)),
                            create.contractKey.map(
                              LfEngineToApi
                                .lfVersionedValueToApiValue(verbose = request.verbose, _)
                                .fold(
                                  err =>
                                    throw new RuntimeException(
                                      s"Unexpected error when converting stored contract: $err"),
                                  identity)),
                            Some(
                              LfEngineToApi
                                .lfValueToApiRecord(
                                  verbose = request.verbose,
                                  create.argument.value)
                                .fold(
                                  err =>
                                    sys.error(
                                      s"Unexpected error when converting stored contract: $err"),
                                  identity)
                            ),
                            create.stakeholders.toSeq
                          )
                        )
                      )
                  }
                  .concat(Source.single(GetActiveContractsResponse(offset = offset.value)))
            }
        }
      )
  }

  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)
}

object ApiActiveContractsService {
  type TransactionId = String
  type WorkflowId = String

  def create(ledgerId: LedgerId, backend: ACSBackend, identifierResolver: IdentifierResolver)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory)
    : ActiveContractsService with BindableService with ActiveContractsServiceLogging =
    new ActiveContractsServiceValidation(
      new ApiActiveContractsService(backend, identifierResolver)(ec, mat, esf),
      ledgerId
    ) with BindableService with ActiveContractsServiceLogging {
      override def bindService(): ServerServiceDefinition =
        ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)
    }
}
