// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v1.{
  AcsUpdateEvent,
  ActiveContractSetSnapshot,
  IndexService
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.active_contracts_service._
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.api.validation.TransactionFilterValidator
import com.digitalasset.platform.api.grpc.GrpcApiService
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.participant.util.{EventFilter, LfEngineToApi}
import com.digitalasset.platform.server.api.validation.{
  ActiveContractsServiceValidation,
  ErrorFactories,
  IdentifierResolver
}
import io.grpc.{BindableService, ServerServiceDefinition}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class DamlOnXActiveContractsService private (
    indexService: IndexService,
    identifierResolver: IdentifierResolver,
    parallelism: Int = Runtime.getRuntime.availableProcessors)(
    implicit executionContext: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends ActiveContractsServiceAkkaGrpc
    with GrpcApiService
    with ErrorFactories
    with DamlOnXServiceUtils {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val txFilterValidator = new TransactionFilterValidator(identifierResolver)

  //@SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  override protected def getActiveContractsSource(
      request: GetActiveContractsRequest): Source[GetActiveContractsResponse, NotUsed] = {

    txFilterValidator
      .validate(request.getFilter, "filter")
      .fold(
        Source.failed, { filter =>
          Source
            .fromFuture(
              consumeAsyncResult(
                indexService.getActiveContractSetSnapshot(
                  Ref.SimpleString.assertFromString(request.ledgerId),
                  filter
                )
              )
            )
            .flatMapConcat {
              (snapshot: ActiveContractSetSnapshot) =>
                snapshot.activeContracts
                  .mapConcat {
                    case (workflowId, createEvent) =>
                      filteredApiContract(
                        EventFilter.byTemplates(filter),
                        workflowId.underlyingString,
                        createEvent,
                        request.verbose).toList
                  }
                  .concat(
                    Source.single(GetActiveContractsResponse(offset = snapshot.takenAt.toString)))
            }

        }
      )
  }

  private def filteredApiContract(
      eventFilter: EventFilter.TemplateAwareFilter,
      workflowId: String,
      a: AcsUpdateEvent.Create,
      verbose: Boolean): Option[GetActiveContractsResponse] = {
    val create = toApiCreated(a, verbose)
    eventFilter
      .filterEvent(Event(create))
      .map(evt =>
        GetActiveContractsResponse(workflowId = workflowId, activeContracts = List(evt.getCreated)))
  }

  private def toApiCreated(a: AcsUpdateEvent.Create, verbose: Boolean): Created = {
    Created(
      CreatedEvent(
        a.contractId.coid, // FIXME(JM): Does EventId == ContractId make sense here?
        a.contractId.coid,
        Some(LfEngineToApi.toApiIdentifier(a.templateId)),
        Some(
          LfEngineToApi
            .lfValueToApiRecord(verbose = verbose, a.argument.value)
            .fold(
              err =>
                throw new RuntimeException(
                  s"Unexpected error when converting stored contract: $err"),
              identity)),
        a.stakeholders.map(_.underlyingString)
      ))
  }

  override def bindService(): ServerServiceDefinition =
    ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)
}

object DamlOnXActiveContractsService {

  def create(indexService: IndexService, identifierResolver: IdentifierResolver)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory)
    : ActiveContractsService with BindableService with ActiveContractsServiceLogging = {

    val ledgerId = Await.result(indexService.getLedgerId(), 5.seconds)

    new ActiveContractsServiceValidation(
      new DamlOnXActiveContractsService(indexService, identifierResolver)(ec, mat, esf),
      ledgerId.underlyingString
    ) with BindableService with ActiveContractsServiceLogging {
      override def bindService(): ServerServiceDefinition =
        ActiveContractsServiceGrpc.bindService(this, DirectExecutionContext)
    }
  }
}
