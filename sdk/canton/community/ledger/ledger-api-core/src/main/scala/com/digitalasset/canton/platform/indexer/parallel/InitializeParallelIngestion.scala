// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.{
  CompletionStorageBackend,
  IngestionStorageBackend,
  ParameterStorageBackend,
  StringInterningStorageBackend,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.interning.UpdatingStringInterningView
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.{ExecutionContext, Future}

private[platform] final case class InitializeParallelIngestion(
    providedParticipantId: Ref.ParticipantId,
    ingestionStorageBackend: IngestionStorageBackend[?],
    parameterStorageBackend: ParameterStorageBackend,
    completionStorageBackend: CompletionStorageBackend,
    stringInterningStorageBackend: StringInterningStorageBackend,
    updatingStringInterningView: UpdatingStringInterningView,
    postProcessor: (Vector[PostPublishData], TraceContext) => Future[Unit],
    metrics: LedgerApiServerMetrics,
    loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  def apply(
      dbDispatcher: DbDispatcher,
      initializeInMemoryState: Option[LedgerEnd] => Future[Unit],
  ): Future[Option[LedgerEnd]] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(logger)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace.empty
    logger.info(s"Attempting to initialize with participant ID $providedParticipantId")
    for {
      _ <- dbDispatcher.executeSql(metrics.index.db.initializeLedgerParameters)(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            participantId = ParticipantId(providedParticipantId)
          ),
          loggerFactory,
        )
      )
      ledgerEnd <- dbDispatcher.executeSql(metrics.index.db.getLedgerEnd)(
        parameterStorageBackend.ledgerEnd
      )
      _ <- dbDispatcher.executeSql(metrics.indexer.initialization)(
        ingestionStorageBackend.deletePartiallyIngestedData(ledgerEnd)
      )
      _ <- updatingStringInterningView.update(ledgerEnd.map(_.lastStringInterningId)) {
        (fromExclusive, toInclusive) =>
          implicit val loggingContext: LoggingContextWithTrace =
            LoggingContextWithTrace.empty
          dbDispatcher.executeSql(metrics.index.db.loadStringInterningEntries) {
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          }
      }
      // post processing recovery should come after initializing string interning when the dependent storage backend operations are running
      postProcessingEndOffset <- dbDispatcher.executeSql(metrics.index.db.getPostProcessingEnd)(
        parameterStorageBackend.postProcessingEnd
      )
      potentiallyNonPostProcessedCompletions <- ledgerEnd.map(_.lastOffset) match {
        case Some(lastOffset) =>
          dbDispatcher.executeSql(
            metrics.index.db.getPostProcessingEnd
          )(
            completionStorageBackend.commandCompletionsForRecovery(
              startInclusive = postProcessingEndOffset.fold(Offset.firstOffset)(_.increment),
              endInclusive = lastOffset,
            )
          )
        case None => Future.successful(Vector.empty)
      }
      _ <- postProcessor(potentiallyNonPostProcessedCompletions, loggingContext.traceContext)
      _ <- dbDispatcher.executeSql(metrics.indexer.postProcessingEndIngestion)(
        parameterStorageBackend.updatePostProcessingEnd(ledgerEnd.map(_.lastOffset))
      )
      _ = logger.info(s"Indexer initialized at $ledgerEnd")
      _ <- initializeInMemoryState(ledgerEnd)
    } yield ledgerEnd
  }
}
