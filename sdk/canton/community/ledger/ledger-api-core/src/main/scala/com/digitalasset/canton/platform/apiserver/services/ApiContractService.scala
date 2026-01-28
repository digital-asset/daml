// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.ledger.api.v2.contract_service.{
  ContractServiceGrpc,
  GetContractRequest,
  GetContractResponse,
}
import com.daml.nonempty.NonEmpty
import com.daml.tracing.Telemetry
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.GrpcApiService
import com.digitalasset.canton.ledger.api.validation.FieldValidator
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.PersistedContractInstance
import com.digitalasset.canton.platform.store.LedgerApiContractStore
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.Projection
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value.ContractId
import io.grpc.ServerServiceDefinition

import scala.concurrent.{ExecutionContext, Future}

final class ApiContractService(
    ledgerApiContractStore: LedgerApiContractStore,
    lfValueTranslation: LfValueTranslation,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ContractServiceGrpc.ContractService
    with GrpcApiService
    with NamedLogging {

  override def getContract(request: GetContractRequest): Future[GetContractResponse] = {
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    (for {
      contractId <- FieldValidator.requireContractId(request.contractId, "contract_id")
      queryingParties <- FieldValidator.requireParties(request.queryingParties.toSet)
    } yield {
      ledgerApiContractStore
        .lookupPersisted(contractId)
        .flatMap(toApiResult(queryingParties, contractId))
    }).fold(
      t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
      identity,
    )
  }

  private def toApiResult(
      queryingParties: Set[LfPartyId],
      contractId: ContractId,
  )(
      persistedContractInstanceO: Option[PersistedContractInstance]
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetContractResponse] = {
    for {
      contactInstance <- persistedContractInstanceO
      witnesses <- NonEmpty.from(
        contactInstance.inst.stakeholders.iterator
          .filter(stakeholder => queryingParties.isEmpty || queryingParties(stakeholder))
          .map(_.toString)
          .toSet
      )
    } yield lfValueTranslation
      .toApiCreatedEvent(
        eventProjectionProperties = EventProjectionProperties(
          verbose = false,
          witnessTemplateProjections = Map(
            // witness wildcard
            None -> Map(
              // template wildcard
              None -> Projection(createdEventBlob = true)
            )
          ),
        )(
          interfaceViewPackageUpgrade =
            // using the original template implementation - no interface views are populated for this endpoint - this limitation is stated on the API
            (_: Ref.ValueRef, originalTemplateImplementation: Ref.ValueRef) =>
              Future.successful(Right(originalTemplateImplementation))
        ),
        fatContractInstance = contactInstance.inst,
        // setting one so is not breaking validation only - this should be not used by the client
        offset = 1,
        // this should be not used by the client
        nodeId = 0,
        // this is only valid if it is the same as the contract's package ID - this limitation is stated on the API
        representativePackageId = contactInstance.inst.templateId.packageId,
        witnesses = witnesses,
        // this cannot be determined so hardcoded - this limitation is state on the API
        acsDelta = false,
      )
      .map(Some(_))
      .map(GetContractResponse(_))
  }.getOrElse(
    Future.failed(
      RequestValidationErrors.NotFound.ContractPayload
        .Reject(contractId)
        .asGrpcError
    )
  )

  override def bindService(): ServerServiceDefinition =
    ContractServiceGrpc.bindService(this, executionContext)

  override def close(): Unit = ()
}
