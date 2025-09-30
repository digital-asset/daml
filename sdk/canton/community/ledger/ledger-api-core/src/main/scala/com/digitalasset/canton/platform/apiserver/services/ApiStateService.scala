// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.state_service.*
import com.daml.logging.entries.LoggingEntries
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.{GrpcApiService, StreamingServiceLifecycleManagement}
import com.digitalasset.canton.ledger.api.validation.ValueValidator.requirePresence
import com.digitalasset.canton.ledger.api.validation.{
  FieldValidator,
  FormatValidator,
  ParticipantOffsetValidator,
}
import com.digitalasset.canton.ledger.participant.state.SyncService
import com.digitalasset.canton.ledger.participant.state.index.{
  IndexActiveContractsService as ACSBackend,
  IndexUpdateService,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.shutdownAsGrpcError
import com.digitalasset.canton.topology.transaction.ParticipantPermission as TopologyParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import io.grpc.ServerServiceDefinition
import io.grpc.stub.StreamObserver
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

final class ApiStateService(
    acsService: ACSBackend,
    syncService: SyncService,
    updateService: IndexUpdateService,
    metrics: LedgerApiServerMetrics,
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
        eventFormatProto <- requirePresence(request.eventFormat, "event_format")
        eventFormat <- FormatValidator.validate(eventFormatProto)

        activeAt <- ParticipantOffsetValidator.validateNonNegative(
          request.activeAtOffset,
          "active_at_offset",
        )
      } yield {
        withEnrichedLoggingContext(telemetry)(
          logging.eventFormat(eventFormat)
        ) { implicit loggingContext =>
          logger.info(
            s"Received request for active contracts: $request, ${loggingContext.serializeFiltered("filters")}."
          )
          acsService
            .getActiveContracts(
              eventFormat = eventFormat,
              activeAt = activeAt,
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
        .via(
          logger.enrichedDebugStream("Responding with active contracts.", activeContractsLoggable)
        )
        .via(logger.logErrorsOnStream)
        .via(StreamMetrics.countElements(metrics.lapi.streams.acs))
    }
  }

  override def getConnectedSynchronizers(
      request: GetConnectedSynchronizersRequest
  ): Future[GetConnectedSynchronizersResponse] = {
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    val result = (for {
      partyO <- FieldValidator
        .optionalString(request.party)(FieldValidator.requirePartyField(_, "party"))
      participantId <- FieldValidator
        .optionalParticipantId(request.participantId, "participant_id")
    } yield SyncService.ConnectedSynchronizerRequest(partyO, participantId))
      .fold(
        t => FutureUnlessShutdown.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        request =>
          syncService
            .getConnectedSynchronizers(request)
            .map(response =>
              GetConnectedSynchronizersResponse(
                response.connectedSynchronizers.map { connectedSynchronizer =>
                  val permissions = connectedSynchronizer.permission match {
                    case Some(TopologyParticipantPermission.Submission) =>
                      Some(ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION)
                    case Some(TopologyParticipantPermission.Observation) =>
                      Some(ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION)
                    case Some(TopologyParticipantPermission.Confirmation) =>
                      Some(ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION)
                    case _ => None
                  }
                  permissions
                    .map(permission =>
                      GetConnectedSynchronizersResponse.ConnectedSynchronizer(
                        synchronizerAlias =
                          connectedSynchronizer.synchronizerAlias.toProtoPrimitive,
                        synchronizerId =
                          connectedSynchronizer.synchronizerId.logical.toProtoPrimitive,
                        permission = permission,
                      )
                    )
                    .getOrElse(
                      GetConnectedSynchronizersResponse.ConnectedSynchronizer(
                        synchronizerAlias =
                          connectedSynchronizer.synchronizerAlias.toProtoPrimitive,
                        synchronizerId =
                          connectedSynchronizer.synchronizerId.logical.toProtoPrimitive,
                        permission = ParticipantPermission.PARTICIPANT_PERMISSION_UNSPECIFIED,
                      )
                    )
                }
              )
            ),
      )
    shutdownAsGrpcError(result)
  }

  override def getLedgerEnd(request: GetLedgerEndRequest): Future[GetLedgerEndResponse] = {
    implicit val traceContext =
      TraceContext.fromDamlTelemetryContext(telemetry.contextFromGrpcThreadLocalContext())
    updateService
      .currentLedgerEnd()
      .map(offset =>
        GetLedgerEndResponse(
          offset.fold(0L)(_.unwrap)
        )
      )
      .thereafter(logger.logErrorsOnCall[GetLedgerEndResponse])
  }

  override def getLatestPrunedOffsets(
      request: GetLatestPrunedOffsetsRequest
  ): Future[GetLatestPrunedOffsetsResponse] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory, telemetry)

    updateService
      .latestPrunedOffset()
      .map { prunedUptoInclusive =>
        GetLatestPrunedOffsetsResponse(
          participantPrunedUpToInclusive = prunedUptoInclusive.fold(0L)(_.unwrap),
          allDivulgedContractsPrunedUpToInclusive = prunedUptoInclusive.fold(0L)(_.unwrap),
        )
      }
      .thereafter(logger.logErrorsOnCall[GetLatestPrunedOffsetsResponse])
  }

  override def bindService(): ServerServiceDefinition =
    StateServiceGrpc.bindService(this, executionContext)

  private def activeContractsLoggable(
      activeContractsResponse: GetActiveContractsResponse
  ): LoggingEntries =
    Option(activeContractsResponse.workflowId)
      .filter(_.nonEmpty)
      .map(workflowId => LoggingEntries(logging.workflowId(workflowId)))
      .getOrElse(LoggingEntries())
}
