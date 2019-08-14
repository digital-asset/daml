// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.services

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.api.server.damlonx.services.backport.EventFilter
import com.daml.ledger.participant.state.index.v1.{
  AcsUpdateEvent,
  ActiveContractSetSnapshot,
  IndexService
}
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.WorkflowId
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.active_contracts_service._
import com.digitalasset.ledger.api.v1.event.Event.Event.Created
import com.digitalasset.ledger.api.v1.event.{CreatedEvent, Event}
import com.digitalasset.ledger.api.validation.TransactionFilterValidator
import com.digitalasset.platform.participant.util.LfEngineToApi
import com.digitalasset.platform.server.api.validation.{
  ActiveContractsServiceValidation,
  ErrorFactories
}
import io.grpc.BindableService
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class DamlOnXActiveContractsService private (
    indexService: IndexService,
    parallelism: Int = Runtime.getRuntime.availableProcessors)(
    implicit executionContext: ExecutionContext,
    protected val mat: Materializer,
    protected val esf: ExecutionSequencerFactory)
    extends ActiveContractsServiceAkkaGrpc
    with ErrorFactories {

  private val logger = LoggerFactory.getLogger(this.getClass)

  //@SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  override protected def getActiveContractsSource(
      request: GetActiveContractsRequest): Source[GetActiveContractsResponse, NotUsed] = {

    TransactionFilterValidator
      .validate(request.getFilter, "filter")
      .fold(
        Source.failed, { filter =>
          Source
            .fromFuture(
              indexService.getActiveContractSetSnapshot(filter)
            )
            .flatMapConcat { (snapshot: ActiveContractSetSnapshot) =>
              snapshot.activeContracts
                .mapConcat {
                  case (workflowId, createEvent) =>
                    filteredApiContract(
                      EventFilter.byTemplates(filter),
                      workflowId,
                      createEvent,
                      request.verbose).toList
                }
                .concat(Source.single(GetActiveContractsResponse(offset = snapshot.takenAt.value)))
            }

        }
      )
  }

  private def filteredApiContract(
      eventFilter: EventFilter.TemplateAwareFilter,
      workflowId: Option[WorkflowId],
      a: AcsUpdateEvent.Create,
      verbose: Boolean): Option[GetActiveContractsResponse] = {
    val create = toApiCreated(a, verbose)
    eventFilter
      .filterEvent(Event(create))
      .map(
        evt =>
          GetActiveContractsResponse(
            workflowId = workflowId.getOrElse(""),
            activeContracts = List(evt.getCreated)))
  }

  private def toApiCreated(a: AcsUpdateEvent.Create, verbose: Boolean): Created = {
    Created(
      CreatedEvent(
        a.contractId.coid, // FIXME(JM): Does EventId == ContractId make sense here?
        a.contractId.coid,
        Some(LfEngineToApi.toApiIdentifier(a.templateId)),
        a.contractKey.map(
          ck =>
            LfEngineToApi.assertOrRuntimeEx(
              "converting stored contract",
              LfEngineToApi
                .lfContractKeyToApiValue(verbose, ck))),
        Some(
          LfEngineToApi.assertOrRuntimeEx(
            "converting stored contract",
            LfEngineToApi
              .lfValueToApiRecord(verbose = verbose, a.argument.value))),
        a.stakeholders.toSeq
      ))
  }
}

import com.digitalasset.ledger.api.domain.LedgerId

object DamlOnXActiveContractsService {

  def create(indexService: IndexService)(
      implicit ec: ExecutionContext,
      mat: Materializer,
      esf: ExecutionSequencerFactory)
    : ActiveContractsService with BindableService with ActiveContractsServiceLogging = {

    val ledgerId = Await.result(indexService.getLedgerId(), 5.seconds)

    new ActiveContractsServiceValidation(
      new DamlOnXActiveContractsService(indexService)(ec, mat, esf),
      LedgerId(ledgerId)
    ) with ActiveContractsServiceLogging
  }
}
