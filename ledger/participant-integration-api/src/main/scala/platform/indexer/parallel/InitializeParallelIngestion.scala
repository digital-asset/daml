// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.domain
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{ReadService, Update}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.backend.{
  IngestionStorageBackend,
  ParameterStorageBackend,
  StringInterningStorageBackend,
}
import com.daml.platform.store.cache.StringInterningCache

import scala.concurrent.{ExecutionContext, Future}

private[platform] case class InitializeParallelIngestion(
    providedParticipantId: Ref.ParticipantId,
    storageBackend: IngestionStorageBackend[_]
      with ParameterStorageBackend
      with StringInterningStorageBackend,
    metrics: Metrics,
) {

  private val logger = ContextualizedLogger.get(classOf[InitializeParallelIngestion])

  def apply(
      dbDispatcher: DbDispatcher,
      readService: ReadService,
      ec: ExecutionContext,
      mat: Materializer,
  )(implicit loggingContext: LoggingContext): Future[InitializeParallelIngestion.Initialized] = {
    implicit val executionContext: ExecutionContext = ec
    for {
      initialConditions <- readService.ledgerInitialConditions().runWith(Sink.head)(mat)
      providedLedgerId = domain.LedgerId(initialConditions.ledgerId)
      _ = logger.info(
        s"Attempting to initialize with ledger ID $providedLedgerId and participant ID $providedParticipantId"
      )
      _ <- dbDispatcher.executeSql(metrics.daml.index.db.initializeLedgerParameters)(
        storageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = providedLedgerId,
            participantId = domain.ParticipantId(providedParticipantId),
          )
        )
      )
      ledgerEnd <- dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEnd)(
        storageBackend.ledgerEnd
      )
      _ <- dbDispatcher.executeSql(metrics.daml.parallelIndexer.initialization)(
        storageBackend.deletePartiallyIngestedData(ledgerEnd)
      )
      allStringInterningEntries <- dbDispatcher.executeSql(
        metrics.daml.parallelIndexer.initialization
      )( // TODO FIXME metrics
        storageBackend.loadStringInterningEntries(
          fromIdExclusive = 0, // TODO FIXME pull out and constantify zero element
          untilIdInclusive = ledgerEnd
            .map(_.lastStringInterningId)
            .getOrElse(0), // TODO FIXME pull out and constantify zero element
        )
      )
    } yield InitializeParallelIngestion.Initialized(
      initialEventSeqId = ledgerEnd.map(_.lastEventSeqId).getOrElse(EventSequentialId.beforeBegin),
      initialStringInterningCache = StringInterningCache.from(allStringInterningEntries),
      readServiceSource = readService.stateUpdates(beginAfter = ledgerEnd.map(_.lastOffset)),
    )
  }

}

object InitializeParallelIngestion {

  case class Initialized(
      initialEventSeqId: Long,
      initialStringInterningCache: StringInterningCache,
      readServiceSource: Source[(Offset, Update), NotUsed],
  )

}
