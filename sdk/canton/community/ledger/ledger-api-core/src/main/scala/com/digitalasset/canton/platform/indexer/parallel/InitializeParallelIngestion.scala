// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.participant.state.{ReadService, Update}
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
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.daml.lf.data.Ref
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

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
      initializeInMemoryState: LedgerEnd => Future[Unit],
      readService: ReadService,
  ): Future[InitializeParallelIngestion.Initialized] = {
    implicit val ec: ExecutionContext = DirectExecutionContext(logger)
    implicit val loggingContext: LoggingContextWithTrace =
      LoggingContextWithTrace.empty
    logger.info(s"Attempting to initialize with participant ID $providedParticipantId")
    for {
      _ <- dbDispatcher.executeSql(metrics.index.db.initializeLedgerParameters)(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            participantId = domain.ParticipantId(providedParticipantId)
          ),
          loggerFactory,
        )
      )
      ledgerEnd <- dbDispatcher.executeSql(metrics.index.db.getLedgerEnd)(
        parameterStorageBackend.ledgerEnd
      )
      _ <- dbDispatcher.executeSql(metrics.parallelIndexer.initialization)(
        ingestionStorageBackend.deletePartiallyIngestedData(ledgerEnd)
      )
      _ <- updatingStringInterningView.update(ledgerEnd.lastStringInterningId) {
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
// TODO(i18695): FIXME on big bang rollout
//      potentiallyNonPostProcessedCompletions <- dbDispatcher.executeSql(
//        metrics.index.db.getPostProcessingEnd
//      )(
//        completionStorageBackend.commandCompletionsForRecovery(
//          startExclusive = postProcessingEndOffset.getOrElse(Offset.beforeBegin),
//          endInclusive = ledgerEnd.lastOffset,
//        )
//      )
//      _ <- postProcessor(potentiallyNonPostProcessedCompletions, loggingContext.traceContext)
      _ <- dbDispatcher.executeSql(metrics.parallelIndexer.postProcessingEndIngestion)(
        parameterStorageBackend.updatePostProcessingEnd(ledgerEnd.lastOffset)
      )
      _ = logger.info(s"Indexer initialized at $ledgerEnd")
      _ <- initializeInMemoryState(ledgerEnd)
    } yield InitializeParallelIngestion.Initialized(
      initialEventSeqId = ledgerEnd.lastEventSeqId,
      initialStringInterningId = ledgerEnd.lastStringInterningId,
      initialLastPublicationTime = ledgerEnd.lastPublicationTime,
      readServiceSource = readService.stateUpdates(beginAfter = ledgerEnd.lastOffsetOption),
    )
  }
}

object InitializeParallelIngestion {

  final case class Initialized(
      initialEventSeqId: Long,
      initialStringInterningId: Int,
      initialLastPublicationTime: CantonTimestamp,
      readServiceSource: Source[(Offset, Traced[Update]), NotUsed],
  )
}
