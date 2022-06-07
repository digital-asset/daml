// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{ReadService, Update}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.ParticipantInMemoryState
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

  def apply(
      dbDispatcher: DbDispatcher,
      participantInMemoryState: ParticipantInMemoryState,
      readService: ReadService,
      ec: ExecutionContext,
  )(implicit loggingContext: LoggingContext): Future[InitializeParallelIngestion.Initialized] = {
    implicit val executionContext: ExecutionContext = ec
    for {
      ledgerEnd <- dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEnd)(
        parameterStorageBackend.ledgerEnd
      )
      _ <- dbDispatcher.executeSql(metrics.daml.parallelIndexer.initialization)(
        ingestionStorageBackend.deletePartiallyIngestedData(ledgerEnd)
      )
      _ <- participantInMemoryState.resetTo(ledgerEnd, dbDispatcher, stringInterningStorageBackend)
    } yield InitializeParallelIngestion.Initialized(
      initialOffset = ledgerEnd.lastOffset,
      initialEventSeqId = ledgerEnd.lastEventSeqId,
      initialStringInterningId = ledgerEnd.lastStringInterningId,
      readServiceSource = readService.stateUpdates(beginAfter = ledgerEnd.lastOffsetOption),
    )
  }
}

object InitializeParallelIngestion {

  case class Initialized(
      initialOffset: Offset,
      initialEventSeqId: Long,
      initialStringInterningId: Int,
      readServiceSource: Source[(Offset, Update), NotUsed],
  )

}
