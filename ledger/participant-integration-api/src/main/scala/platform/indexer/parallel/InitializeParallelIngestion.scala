// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{ReadService, Update}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.backend.IngestionStorageBackend

import scala.concurrent.{ExecutionContext, Future}

private[platform] case class InitializeParallelIngestion(
    storageBackend: IngestionStorageBackend[_],
    metrics: Metrics,
) {

  def apply(
      dbDispatcher: DbDispatcher,
      readService: ReadService,
      ec: ExecutionContext,
  )(implicit loggingContext: LoggingContext): Future[InitializeParallelIngestion.Initialized] = {
    dbDispatcher
      .executeSql(metrics.daml.parallelIndexer.initialization)(
        storageBackend.initializeIngestion
      )
      .map(ledgerEnd =>
        InitializeParallelIngestion.Initialized(
          initialEventSeqId =
            ledgerEnd.map(_.lastEventSeqId).getOrElse(EventSequentialId.beforeBegin),
          readServiceSource = readService.stateUpdates(beginAfter = ledgerEnd.map(_.lastOffset)),
        )
      )(ec)
  }

}

object InitializeParallelIngestion {

  case class Initialized(
      initialEventSeqId: Long,
      readServiceSource: Source[(Offset, Update), NotUsed],
  )

}
