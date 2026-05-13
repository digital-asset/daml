// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import cats.data.OptionT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.update_service.*
import com.daml.logging.entries.LoggingEntries
import com.daml.tracing.Telemetry
import com.digitalasset.canton.ledger.api.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.ledger.api.validation.UpdateServiceRequestValidator
import com.digitalasset.canton.ledger.api.{UpdateFormat, ValidationLogger}
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.TracedLoggerOps.TracedLoggerOps
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.protocol.UpdateId
import com.digitalasset.canton.util.Thereafter.syntax.*
import io.grpc.stub.StreamObserver
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

final class ApiUpdateService(
    updateService: IndexUpdateService,
    metrics: LedgerApiServerMetrics,
    telemetry: Telemetry,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    esf: ExecutionSequencerFactory,
    executionContext: ExecutionContext,
    mat: Materializer,
) extends UpdateServiceGrpc.UpdateService
    with StreamingServiceLifecycleManagement
    with NamedLogging {

  override def getUpdates(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdatesResponse],
  ): Unit = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    registerStream(responseObserver) {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext(logger, loggingContextWithTrace)

      logger.debug(s"Received new update request $request.")
      Source.future(updateService.currentLedgerEnd()).flatMapConcat { ledgerEnd =>
        val validation = UpdateServiceRequestValidator.validate(request, ledgerEnd)

        validation.fold(
          t => Source.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
          req => {
            LoggingContextWithTrace.withEnrichedLoggingContext(
              logging.startExclusive(req.startExclusive),
              logging.endInclusive(req.endInclusive),
              logging.updateFormat(req.updateFormat),
            ) { implicit loggingContext =>
              logger.info(
                s"Received request for updates, ${loggingContext
                    .serializeFiltered("startExclusive", "endInclusive", "updateFormat")}."
              )(loggingContext.traceContext)
            }
            logger.trace(s"Update request: $req.")
            updateService
              .updates(req.startExclusive, req.endInclusive, req.updateFormat)
              .via(logger.enrichedDebugStream("Responding with updates.", updatesLoggable))
              .via(logger.logErrorsOnStream)
              .via(StreamMetrics.countElements(metrics.lapi.streams.updates))
          },
        )
      }
    }
  }

  override def getUpdateByOffset(
      req: GetUpdateByOffsetRequest
  ): Future[GetUpdateResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContextWithTrace)

    UpdateServiceRequestValidator
      .validateUpdateByOffset(req)
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, req, t)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace.enriched(
              logging.offset(request.offset.unwrap),
              logging.updateFormat(request.updateFormat),
            )(loggingContextWithTrace)
          logger.info(s"Received request for update by offset, ${enrichedLoggingContext
              .serializeFiltered("offset", "updateFormat")}.")(loggingContextWithTrace.traceContext)
          logger.trace(s"Update by offset request: $request")(
            loggingContextWithTrace.traceContext
          )
          val offset = request.offset
          OptionT(
            updateService.getUpdateBy(LookupKey.ByOffset(offset), request.updateFormat)(
              loggingContextWithTrace
            )
          )
            .getOrElseF(
              Future.failed(
                RequestValidationErrors.NotFound.Update.RejectWithOffset(offset.unwrap).asGrpcError
              )
            )
            .thereafter(
              logger.logErrorsOnCall[GetUpdateResponse](loggingContextWithTrace.traceContext)
            )
        },
      )
  }

  override def getUpdateById(
      req: GetUpdateByIdRequest
  ): Future[GetUpdateResponse] = {
    val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    UpdateServiceRequestValidator
      .validateUpdateById(req)(errorLoggingContext)
      .fold(
        t =>
          Future
            .failed(ValidationLogger.logFailureWithTrace(logger, req, t)(loggingContextWithTrace)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace.enriched(
              logging.updateId(request.updateId),
              logging.updateFormat(request.updateFormat),
            )(loggingContextWithTrace)
          logger.info(
            s"Received request for update by ID, ${enrichedLoggingContext
                .serializeFiltered("eventId", "updateFormat")}."
          )(loggingContextWithTrace.traceContext)
          logger.trace(s"Update by ID request: $request")(loggingContextWithTrace.traceContext)

          internalGetUpdateById(request.updateId, request.updateFormat)
            .thereafter(
              logger.logErrorsOnCall[GetUpdateResponse](loggingContextWithTrace.traceContext)
            )
        },
      )
  }

  private def internalGetUpdateById(
      updateId: UpdateId,
      updateFormat: UpdateFormat,
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Future[GetUpdateResponse] =
    OptionT(updateService.getUpdateBy(LookupKey.ByUpdateId(updateId), updateFormat))
      .getOrElseF(
        Future.failed(
          RequestValidationErrors.NotFound.Update
            .RejectWithTxId(updateId.toHexString)
            .asGrpcError
        )
      )

  private def updatesLoggable(updates: GetUpdatesResponse): LoggingEntries =
    updates.update match {
      case GetUpdatesResponse.Update.Transaction(t) =>
        entityLoggable(t.commandId, t.updateId, t.workflowId, t.offset)
      case GetUpdatesResponse.Update.Reassignment(r) =>
        entityLoggable(r.commandId, r.updateId, r.workflowId, r.offset)
      case GetUpdatesResponse.Update.OffsetCheckpoint(c) =>
        LoggingEntries(logging.offset(c.offset))
      case GetUpdatesResponse.Update.TopologyTransaction(tt) =>
        LoggingEntries(logging.offset(tt.offset))
      case GetUpdatesResponse.Update.Empty =>
        LoggingEntries()
    }

  private def entityLoggable(
      commandId: String,
      updateId: String,
      workflowId: String,
      offset: Long,
  ): LoggingEntries =
    LoggingEntries(
      logging.commandId(commandId),
      logging.updateId(updateId),
      logging.workflowId(workflowId),
      logging.offset(offset),
    )
}
