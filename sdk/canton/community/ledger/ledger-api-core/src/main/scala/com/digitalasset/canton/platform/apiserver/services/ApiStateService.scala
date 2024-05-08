// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.state_service.*
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.{GrpcApiService, StreamingServiceLifecycleManagement}
import com.digitalasset.canton.ledger.api.validation.{FieldValidator, TransactionFilterValidator}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.ReadService
import com.digitalasset.canton.ledger.participant.state.index.{
  IndexActiveContractsService as ACSBackend,
  IndexTransactionsService,
}
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.ApiOffset
import com.digitalasset.canton.topology.transaction.ParticipantPermission as TopologyParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

final class ApiStateService(
    acsService: ACSBackend,
    readService: ReadService,
    txService: IndexTransactionsService,
    metrics: Metrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    mat: Materializer,
    esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
) extends StateServiceGrpc.StateService
    with StreamingServiceLifecycleManagement
    with GrpcApiService
    with NamedLogging {

  override def getActiveContracts(
      request: GetActiveContractsRequest,
      responseObserver: StreamObserver[GetActiveContractsResponse],
  ): Unit = {
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    registerStream(responseObserver) {

      val result = for {
        filters <- TransactionFilterValidator.validate(
          TransactionFilter(request.getFilter.filtersByParty)
        )
        activeAtO <- FieldValidator.optionalString(request.activeAtOffset)(str =>
          ApiOffset.fromString(str).left.map { errorMsg =>
            RequestValidationErrors.NonHexOffset
              .Error(
                fieldName = "active_at_offset",
                offsetValue = request.activeAtOffset,
                message = s"Reason: $errorMsg",
              )
              .asGrpcError
          }
        )
      } yield {
        withEnrichedLoggingContext(telemetry)(
          logging.filters(filters)
        ) { implicit loggingContext =>
          logger.info(
            s"Received request for active contracts: $request, ${loggingContext.serializeFiltered("filters")}."
          )
          acsService
            .getActiveContracts(
              filter = filters,
              verbose = request.verbose,
              activeAtO = activeAtO,
            )
        }
      }
      result
        .fold(
          t =>
            Source.failed(
              ValidationLogger.logFailureWithTrace(logger, request, t)
            ),
          identity,
        )
        .via(logger.logErrorsOnStream)
        .via(StreamMetrics.countElements(metrics.lapi.streams.acs))
    }
  }

  override def getConnectedDomains(
      request: GetConnectedDomainsRequest
  ): Future[GetConnectedDomainsResponse] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)
    FieldValidator
      .requirePartyField(request.party, "party")
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        party =>
          readService
            .getConnectedDomains(ReadService.ConnectedDomainRequest(party))
            .map(response =>
              GetConnectedDomainsResponse(
                response.connectedDomains.flatMap { connectedDomain =>
                  val permissions = connectedDomain.permission match {
                    case TopologyParticipantPermission.Submission =>
                      Seq(ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION)
                    case TopologyParticipantPermission.Observation =>
                      Seq(ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION)
                    case TopologyParticipantPermission.Confirmation =>
                      Seq(ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION)
                    case _ => Nil
                  }
                  permissions.map(permission =>
                    GetConnectedDomainsResponse.ConnectedDomain(
                      domainAlias = connectedDomain.domainAlias.toProtoPrimitive,
                      domainId = connectedDomain.domainId.toProtoPrimitive,
                      permission = permission,
                    )
                  )
                }
              )
            ),
      )
  }

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] = {
    implicit val traceContext =
      TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
    txService
      .currentLedgerEnd()
      .map(offset =>
        GetLedgerEndResponse(
          Some(
            ParticipantOffset(
              ParticipantOffset.Value.Absolute(offset.value)
            )
          )
        )
      )
      .andThen(logger.logErrorsOnCall[GetLedgerEndResponse])
  }

  override def getLatestPrunedOffsets(
      request: GetLatestPrunedOffsetsRequest
  ): Future[GetLatestPrunedOffsetsResponse] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)

    txService
      .latestPrunedOffsets()
      .map { case (prunedUptoInclusive, divulgencePrunedUptoInclusive) =>
        GetLatestPrunedOffsetsResponse(
          participantPrunedUpToInclusive =
            Some(ParticipantOffset(ParticipantOffset.Value.Absolute(prunedUptoInclusive.value))),
          allDivulgedContractsPrunedUpToInclusive = Some(
            ParticipantOffset(ParticipantOffset.Value.Absolute(divulgencePrunedUptoInclusive.value))
          ),
        )
      }
      .andThen(logger.logErrorsOnCall[GetLatestPrunedOffsetsResponse])
  }

  override def bindService(): ServerServiceDefinition =
    StateServiceGrpc.bindService(this, executionContext)

}
