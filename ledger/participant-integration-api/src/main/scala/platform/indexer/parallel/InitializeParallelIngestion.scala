// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend.{
  IngestionStorageBackend,
  ParameterStorageBackend,
  StringInterningStorageBackend,
}
import com.daml.platform.store.dao.DbDispatcher

import scala.concurrent.{ExecutionContext, Future}

private[platform] case class InitializeParallelIngestion(
    providedParticipantId: Ref.ParticipantId,
    ingestionStorageBackend: IngestionStorageBackend[_],
    parameterStorageBackend: ParameterStorageBackend,
    stringInterningStorageBackend: StringInterningStorageBackend,
    metrics: Metrics,
) {

  private val logger = ContextualizedLogger.get(classOf[InitializeParallelIngestion])

  def apply(
      dbDispatcher: DbDispatcher,
      additionalInitialization: LedgerEnd => Future[Unit],
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
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = providedLedgerId,
            participantId = domain.ParticipantId(providedParticipantId),
          )
        )
      )
      ledgerEnd <- dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEnd)(
        parameterStorageBackend.ledgerEnd
      )
      _ <- dbDispatcher.executeSql(metrics.daml.parallelIndexer.initialization)(
        ingestionStorageBackend.deletePartiallyIngestedData(ledgerEnd)
      )
      _ <- additionalInitialization(ledgerEnd)
    } yield InitializeParallelIngestion.Initialized(
      initialEventSeqId = ledgerEnd.lastEventSeqId,
      initialStringInterningId = ledgerEnd.lastStringInterningId,
      readServiceSource = readService.stateUpdates(beginAfter = ledgerEnd.lastOffsetOption),
    )
  }
}

object InitializeParallelIngestion {

  case class Initialized(
      initialEventSeqId: Long,
      initialStringInterningId: Int,
      readServiceSource: Source[(Offset, Update), NotUsed],
  )
}
