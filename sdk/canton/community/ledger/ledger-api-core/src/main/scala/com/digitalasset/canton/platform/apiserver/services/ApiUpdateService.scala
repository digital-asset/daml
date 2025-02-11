// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import cats.data.OptionT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.update_service.*
import com.daml.logging.entries.LoggingEntries
import com.daml.tracing.Telemetry
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape.AcsDelta
import com.digitalasset.canton.ledger.api.grpc.StreamingServiceLifecycleManagement
import com.digitalasset.canton.ledger.api.validation.UpdateServiceRequestValidator
import com.digitalasset.canton.ledger.api.{UpdateId, ValidationLogger}
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
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.{
  InternalEventFormat,
  InternalTransactionFormat,
  TemplatePartiesFilter,
}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.data.Ref.Party
import io.grpc.stub.StreamObserver
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import scalaz.syntax.tag.*

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
        val validation = UpdateServiceRequestValidator.validate(
          GetUpdatesRequest(
            beginExclusive = request.beginExclusive,
            endInclusive = request.endInclusive,
            filter = request.filter,
            verbose = request.verbose,
          ),
          ledgerEnd,
        )

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

  override def getUpdateTrees(
      request: GetUpdatesRequest,
      responseObserver: StreamObserver[GetUpdateTreesResponse],
  ): Unit = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    registerStream(responseObserver) {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext(logger, loggingContextWithTrace)

      logger.debug(s"Received new update trees request $request.")
      Source.future(updateService.currentLedgerEnd()).flatMapConcat { ledgerEnd =>
        val validation = UpdateServiceRequestValidator.validateForTrees(
          GetUpdatesRequest(
            beginExclusive = request.beginExclusive,
            endInclusive = request.endInclusive,
            filter = request.filter,
            verbose = request.verbose,
          ),
          ledgerEnd,
        )

        validation.fold(
          t => Source.failed(ValidationLogger.logFailureWithTrace(logger, request, t)),
          req =>
            if (
              req.eventFormat.filtersByParty.isEmpty && req.eventFormat.filtersForAnyParty.isEmpty
            ) {
              logger.debug("transaction filters were empty, will not return anything")
              Source.empty
            } else {
              LoggingContextWithTrace.withEnrichedLoggingContext(
                logging.startExclusive(req.startExclusive),
                logging.endInclusive(req.endInclusive),
                logging.eventFormat(req.eventFormat),
              ) { implicit loggingContext =>
                logger.info(
                  s"Received request for update trees, ${loggingContext
                      .serializeFiltered("startExclusive", "endInclusive", "updateFormat")}."
                )(loggingContext.traceContext)
              }
              logger.trace(s"Update tree request: $req.")
              updateService
                .transactionTrees(
                  req.startExclusive,
                  req.endInclusive,
                  req.eventFormat,
                )
                .via(
                  logger.enrichedDebugStream("Responding with update trees.", updatesLoggable)
                )
                .via(logger.logErrorsOnStream)
                .via(StreamMetrics.countElements(metrics.lapi.streams.updateTrees))
            },
        )
      }
    }
  }

  override def getTransactionTreeByOffset(
      req: GetTransactionByOffsetRequest
  ): Future[GetTransactionTreeResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContextWithTrace)

    UpdateServiceRequestValidator
      .validateTransactionByOffset(req)
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, req, t)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace.enriched(
              logging.offset(request.offset.unwrap),
              logging.parties(request.requestingParties),
            )(loggingContextWithTrace)
          logger.info(s"Received request for transaction tree by offset, ${enrichedLoggingContext
              .serializeFiltered("offset", "parties")}.")(loggingContextWithTrace.traceContext)
          logger.trace(s"Transaction tree by offset request: $request")(
            loggingContextWithTrace.traceContext
          )
          val offset = request.offset
          updateService
            .getTransactionTreeByOffset(offset, request.requestingParties)(
              loggingContextWithTrace
            )
            .flatMap {
              case None =>
                Future.failed(
                  RequestValidationErrors.NotFound.Transaction
                    .RejectWithOffset(offset.unwrap)
                    .asGrpcError
                )
              case Some(transactionTree) =>
                Future.successful(transactionTree)
            }
            .thereafter(
              logger
                .logErrorsOnCall[GetTransactionTreeResponse](loggingContextWithTrace.traceContext)
            )
        },
      )
  }

  override def getTransactionTreeById(
      req: GetTransactionByIdRequest
  ): Future[GetTransactionTreeResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContextWithTrace)

    UpdateServiceRequestValidator
      .validateTransactionById(req)
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, req, t)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace.enriched(
              logging.updateId(request.updateId),
              logging.parties(request.requestingParties),
            )(loggingContextWithTrace)
          logger.info(s"Received request for transaction tree by ID, ${enrichedLoggingContext
              .serializeFiltered("eventId", "parties")}.")(loggingContextWithTrace.traceContext)
          logger.trace(s"Transaction tree by ID request: $request")(
            loggingContextWithTrace.traceContext
          )
          updateService
            .getTransactionTreeById(request.updateId, request.requestingParties)(
              loggingContextWithTrace
            )
            .flatMap {
              case None =>
                Future.failed(
                  RequestValidationErrors.NotFound.Transaction
                    .RejectWithTxId(request.updateId.unwrap)
                    .asGrpcError
                )
              case Some(transactionTree) =>
                Future.successful(transactionTree)
            }
            .thereafter(
              logger
                .logErrorsOnCall[GetTransactionTreeResponse](loggingContextWithTrace.traceContext)
            )
        },
      )
  }

  override def getTransactionByOffset(
      req: GetTransactionByOffsetRequest
  ): Future[GetTransactionResponse] = {
    implicit val loggingContextWithTrace: LoggingContextWithTrace =
      LoggingContextWithTrace(loggerFactory, telemetry)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext(logger, loggingContextWithTrace)

    UpdateServiceRequestValidator
      .validateTransactionByOffset(req)
      .fold(
        t => Future.failed(ValidationLogger.logFailureWithTrace(logger, req, t)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace.enriched(
              logging.offset(request.offset.unwrap),
              logging.parties(request.requestingParties),
            )(loggingContextWithTrace)
          logger.info(s"Received request for transaction by offset, ${enrichedLoggingContext
              .serializeFiltered("offset", "parties")}.")(loggingContextWithTrace.traceContext)
          logger.trace(s"Transaction by offset request: $request")(
            loggingContextWithTrace.traceContext
          )
          val offset = request.offset
          internalGetTransactionByOffset(offset, request.requestingParties)(
            loggingContextWithTrace
          ).thereafter(
            logger.logErrorsOnCall[GetTransactionResponse](loggingContextWithTrace.traceContext)
          )
        },
      )
  }

  override def getTransactionById(
      req: GetTransactionByIdRequest
  ): Future[GetTransactionResponse] = {
    val loggingContextWithTrace = LoggingContextWithTrace(loggerFactory, telemetry)
    val errorLoggingContext = ErrorLoggingContext(logger, loggingContextWithTrace)

    UpdateServiceRequestValidator
      .validateTransactionById(req)(errorLoggingContext)
      .fold(
        t =>
          Future
            .failed(ValidationLogger.logFailureWithTrace(logger, req, t)(loggingContextWithTrace)),
        request => {
          implicit val enrichedLoggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace.enriched(
              logging.updateId(request.updateId),
              logging.parties(request.requestingParties),
            )(loggingContextWithTrace)
          logger.info(s"Received request for transaction by ID, ${enrichedLoggingContext
              .serializeFiltered("eventId", "parties")}.")(loggingContextWithTrace.traceContext)
          logger.trace(s"Transaction by ID request: $request")(loggingContextWithTrace.traceContext)

          internalGetTransactionById(request.updateId, request.requestingParties)
            .thereafter(
              logger.logErrorsOnCall[GetTransactionResponse](loggingContextWithTrace.traceContext)
            )
        },
      )
  }

  private def emptyTransactionFromTransactionTree(
      tree: GetTransactionTreeResponse
  ): GetTransactionResponse =
    GetTransactionResponse(
      tree.transaction.map(transaction =>
        Transaction(
          updateId = transaction.updateId,
          commandId = transaction.commandId,
          workflowId = transaction.workflowId,
          effectiveAt = transaction.effectiveAt,
          events = Seq.empty,
          offset = transaction.offset,
          synchronizerId = transaction.synchronizerId,
          traceContext = transaction.traceContext,
          recordTime = transaction.recordTime,
        )
      )
    )

  private def internalGetTransactionById(
      updateId: UpdateId,
      requestingParties: Set[Party],
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Future[GetTransactionResponse] = {
    val internalTransactionFormat = InternalTransactionFormat(
      internalEventFormat = InternalEventFormat(
        templatePartiesFilter = TemplatePartiesFilter(
          relation = Map.empty,
          templateWildcardParties = Some(requestingParties),
        ),
        eventProjectionProperties = EventProjectionProperties(
          verbose = true,
          templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
        ),
      ),
      transactionShape = AcsDelta,
    )

    OptionT(updateService.getTransactionById(updateId, internalTransactionFormat))
      .orElse {
        logger.debug(
          s"Transaction not found in flat transaction lookup for updateId $updateId and requestingParties $requestingParties, falling back to transaction tree lookup."
        )
        // When a command submission completes successfully,
        // the submitters can end up getting a TRANSACTION_NOT_FOUND when querying its corresponding flat transaction that either:
        // * has only non-consuming events
        // * has only events of contracts which have stakeholders that are not amongst the requestingParties
        // In these situations, we fallback to a transaction tree lookup and populate the flat transaction response
        // with its details but no events.
        OptionT(updateService.getTransactionTreeById(updateId, requestingParties))
          .map(emptyTransactionFromTransactionTree)
      }
      .getOrElseF(
        Future.failed(
          RequestValidationErrors.NotFound.Transaction.RejectWithTxId(updateId.unwrap).asGrpcError
        )
      )
  }

  private def internalGetTransactionByOffset(
      offset: Offset,
      requestingParties: Set[Party],
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): Future[GetTransactionResponse] = {
    val internalTransactionFormat = InternalTransactionFormat(
      internalEventFormat = InternalEventFormat(
        templatePartiesFilter = TemplatePartiesFilter(
          relation = Map.empty,
          templateWildcardParties = Some(requestingParties),
        ),
        eventProjectionProperties = EventProjectionProperties(
          verbose = true,
          templateWildcardWitnesses = Some(requestingParties.map(_.toString)),
        ),
      ),
      transactionShape = AcsDelta,
    )

    OptionT(updateService.getTransactionByOffset(offset, internalTransactionFormat))
      .orElse {
        logger.debug(
          s"Transaction not found in flat transaction lookup for offset $offset and requestingParties $requestingParties, falling back to transaction tree lookup."
        )
        // When a command submission completes successfully,
        // the submitters can end up getting a TRANSACTION_NOT_FOUND when querying its corresponding flat transaction that either:
        // * has only non-consuming events
        // * has only events of contracts which have stakeholders that are not amongst the requestingParties
        // In these situations, we fallback to a transaction tree lookup and populate the flat transaction response
        // with its details but no events.
        OptionT(updateService.getTransactionTreeByOffset(offset, requestingParties))
          .map(emptyTransactionFromTransactionTree)
      }
      .getOrElseF(
        Future.failed(
          RequestValidationErrors.NotFound.Transaction.RejectWithOffset(offset.unwrap).asGrpcError
        )
      )
  }

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

  private def updatesLoggable(updates: GetUpdateTreesResponse): LoggingEntries =
    updates.update match {
      case GetUpdateTreesResponse.Update.TransactionTree(t) =>
        entityLoggable(t.commandId, t.updateId, t.workflowId, t.offset)
      case GetUpdateTreesResponse.Update.Reassignment(r) =>
        entityLoggable(r.commandId, r.updateId, r.workflowId, r.offset)
      case GetUpdateTreesResponse.Update.OffsetCheckpoint(c) =>
        LoggingEntries(logging.offset(c.offset))
      case GetUpdateTreesResponse.Update.TopologyTransaction(tt) =>
        LoggingEntries(logging.offset(tt.offset))
      case GetUpdateTreesResponse.Update.Empty =>
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
