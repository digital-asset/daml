// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.active_contracts_service._
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.api.validation.TransactionFilterValidator
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.participant.util.{EventFilter, LfEngineToApi}
import com.digitalasset.platform.server.api.validation.{
  ActiveContractsServiceValidation,
  IdentifierResolver
}
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.ledger.backend.api.v1.{ActiveContract, LedgerBackend, LedgerSyncOffset}
import io.grpc.{BindableService, ServerServiceDefinition}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

class SandboxActiveContractsService private (
    getSnapshot: () => Future[(LedgerSyncOffset, Source[ActiveContract, NotUsed])],
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
            .fromFuture(getSnapshot())
            .flatMapConcat {
              case (offset, acsStream) =>
                acsStream
                  .mapConcat { a =>
                    filteredApiContract(EventFilter.byTemplates(filter), a, request.verbose).toList
                  }
                  .concat(Source.single(GetActiveContractsResponse(offset = offset.toString)))
            }
        }
      )
  }

  private def filteredApiContract(
      eventFilter: EventFilter.TemplateAwareFilter,
      a: ActiveContract,
      verbose: Boolean) = {
    val create = toApiCreated(a, verbose)
    eventFilter
      .filterEvent(Event(create))
      .map(
        evt =>
          GetActiveContractsResponse(
            workflowId = a.workflowId,
            activeContracts = List(evt.getCreated)))
  }

  private def toApiCreated(a: ActiveContract, verbose: Boolean): Created = {
    Created(
      CreatedEvent(
        // we use absolute contract ids as event ids throughout the sandbox
        a.contractId.coid,
        a.contractId.coid,
        Some(LfEngineToApi.toApiIdentifier(a.contract.template)),
        Some(
          LfEngineToApi
            .lfValueToApiRecord(verbose = verbose, a.contract.arg.value)
            .fold(
              err =>
                throw new RuntimeException(
                  s"Unexpected error when converting stored contract: $err"),
              identity)),
        a.witnesses.map(_.toString).toSeq
      ))
  }

  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)
}

object SandboxActiveContractsService {
  type TransactionId = String
  type WorkflowId = String

  def apply(ledgerBackend: LedgerBackend, identifierResolver: IdentifierResolver)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory)
    : ActiveContractsService with BindableService with ActiveContractsServiceLogging =
    new ActiveContractsServiceValidation(
      new SandboxActiveContractsService(
        () => ledgerBackend.activeContractSetSnapshot(),
        identifierResolver)(ec, mat, esf),
      ledgerBackend.ledgerId
    ) with BindableService with ActiveContractsServiceLogging {
      override def bindService(): ServerServiceDefinition =
        ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)
    }
}
