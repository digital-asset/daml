// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

object BatchingParallelIngestionPipe {
  def apply[IN, IN_BATCH, DB_BATCH](
      submissionBatchSize: Long,
      batchWithinMillis: Long,
      inputMappingParallelism: Int,
      inputMapper: Iterable[IN] => Future[IN_BATCH],
      seqMapperZero: IN_BATCH,
      seqMapper: (IN_BATCH, IN_BATCH) => IN_BATCH,
      batchingParallelism: Int,
      batcher: IN_BATCH => Future[DB_BATCH],
      ingestingParallelism: Int,
      ingester: DB_BATCH => Future[DB_BATCH],
      tailer: (DB_BATCH, DB_BATCH) => DB_BATCH,
      keepAlive: () => DB_BATCH,
      synchronizeLedgerEndTailIngestion: () => DB_BATCH => Iterator[DB_BATCH],
      tailingRateLimitPerSecond: Int,
      ingestTail: DB_BATCH => Future[DB_BATCH],
  )(source: Source[IN, NotUsed]): Source[Unit, NotUsed] = {
    val _ = tailingRateLimitPerSecond // TODO LLP
    // Stage 1: the stream coming from ReadService, involves deserialization and translation to Update-s
    source
      // Stage 2: Batching plus mapping to Database DTOs encapsulates all the CPU intensive computation of the ingestion. Executed in parallel.
      .groupedWithin(submissionBatchSize.toInt, FiniteDuration(batchWithinMillis, "millis"))
      .mapAsync(inputMappingParallelism)(inputMapper)
      // Stage 3: Encapsulates sequential/stateful computation (generation of sequential IDs for events)
      .scan(seqMapperZero)(seqMapper)
      .drop(1) // remove the zero element from the beginning of the stream
      // Stage 4: Mapping to Database specific representation, encapsulates all database specific preparation of the data. Executed in parallel.
      .async
      .mapAsync(batchingParallelism)(batcher)
      // Stage 5: Inserting data into the database. Almost no CPU load here, threads are executing SQL commands over JDBC, and waiting for the result. This defines the parallelism on the SQL database side, same amount of PostgreSQL Backend processes will do the ingestion work.
      .async
      .mapAsync(ingestingParallelism)(ingester)
      .keepAlive(100.millis, keepAlive)
      // Stage 6: Preparing data sequentially for throttled mutations in database (tracking the ledger-end, corresponding sequential event ids and latest-at-the-time configurations)
      .statefulMapConcat(synchronizeLedgerEndTailIngestion)
      .conflate(tailer)
      // Stage 7: Updating ledger-end and related data in database (this stage completion demarcates the consistent point-in-time)
      .mapAsync(1)(ingestTail)
      .map(_ => ())
  }
}
