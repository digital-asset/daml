// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.state_service.*
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.ValidationLogger
import com.digitalasset.canton.ledger.api.grpc.{GrpcApiService, StreamingServiceLifecycleManagement}
import com.digitalasset.canton.ledger.api.validation.{
  FieldValidator,
  FormatValidator,
  ParticipantOffsetValidator,
  ValidationErrors,
}
import com.digitalasset.canton.ledger.participant.state.SyncService
import com.digitalasset.canton.ledger.participant.state.index.{
  IndexActiveContractsService as ACSBackend,
  IndexTransactionsService,
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
    txService: IndexTransactionsService,
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
        filters <- (request.filter, request.verbose, request.eventFormat) match {
          case (Some(_), _, Some(_)) =>
            Left(
              ValidationErrors.invalidArgument(
                s"Both filter/verbose and event_format is specified. Please use either backwards compatible arguments (filter and verbose) or event_format, but not both."
              )
            )

          case (Some(legacyFilter), legacyVerbose, None) =>
            FormatValidator.validate(legacyFilter, legacyVerbose)

          case (None, true, Some(_)) =>
            Left(
              ValidationErrors.invalidArgument(
                s"Both filter/verbose and event_format is specified. Please use either backwards compatible arguments (filter and verbose) or event_format, but not both."
              )
            )

          case (None, false, Some(eventFormat)) =>
            FormatValidator.validate(eventFormat)

          case (None, _, None) =>
            Left(
              ValidationErrors.invalidArgument(
                s"Either filter/verbose or event_format is required. Please use either backwards compatible arguments (filter and verbose) or event_format, but not both."
              )
            )
        }

        activeAt <- ParticipantOffsetValidator.validateNonNegative(
          request.activeAtOffset,
          "active_at_offset",
        )
      } yield {
        withEnrichedLoggingContext(telemetry)(
          logging.eventFormat(filters)
        ) { implicit loggingContext =>
          logger.info(
            s"Received request for active contracts: $request, ${loggingContext.serializeFiltered("filters")}."
          )
          acsService
            .getActiveContracts(
              filter = filters,
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
      party <- FieldValidator
        .requirePartyField(request.party, "party")
      participantId <- FieldValidator
        .optionalParticipantId(request.participantId, "participant_id")
    } yield SyncService.ConnectedSynchronizerRequest(party, participantId))
      .fold(
        t => FutureUnlessShutdown.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
        request =>
          syncService
            .getConnectedSynchronizers(request)
            .map(response =>
              GetConnectedSynchronizersResponse(
                response.connectedSynchronizers.flatMap { connectedSynchronizer =>
                  val permissions = connectedSynchronizer.permission match {
                    case TopologyParticipantPermission.Submission =>
                      Seq(ParticipantPermission.PARTICIPANT_PERMISSION_SUBMISSION)
                    case TopologyParticipantPermission.Observation =>
                      Seq(ParticipantPermission.PARTICIPANT_PERMISSION_OBSERVATION)
                    case TopologyParticipantPermission.Confirmation =>
                      Seq(ParticipantPermission.PARTICIPANT_PERMISSION_CONFIRMATION)
                    case _ => Nil
                  }
                  permissions.map(permission =>
                    GetConnectedSynchronizersResponse.ConnectedSynchronizer(
                      synchronizerAlias = connectedSynchronizer.synchronizerAlias.toProtoPrimitive,
                      synchronizerId = connectedSynchronizer.synchronizerId.toProtoPrimitive,
                      permission = permission,
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
    txService
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

    txService
      .latestPrunedOffsets()
      .map { case (prunedUptoInclusive, divulgencePrunedUptoInclusive) =>
        GetLatestPrunedOffsetsResponse(
          participantPrunedUpToInclusive = prunedUptoInclusive.fold(0L)(_.unwrap),
          allDivulgedContractsPrunedUpToInclusive = divulgencePrunedUptoInclusive.fold(0L)(_.unwrap),
        )
      }
      .thereafter(logger.logErrorsOnCall[GetLatestPrunedOffsetsResponse])
  }

  override def bindService(): ServerServiceDefinition =
    StateServiceGrpc.bindService(this, executionContext)

}
