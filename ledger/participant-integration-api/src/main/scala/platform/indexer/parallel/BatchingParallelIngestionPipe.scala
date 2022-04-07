// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker

import scala.concurrent.Future
import scala.concurrent.duration._

object BatchingParallelIngestionPipe {
  def apply[IN, IN_BATCH, DB_BATCH](
      submissionBatchSize: Long,
      batchWithinMillis: Long,
      inputMappingParallelism: Int,
      inputMapper: Iterable[IN] => Future[((Iterable[IN], LedgerEndMarker), IN_BATCH)],
      seqMapperZero: ((Iterable[IN], LedgerEndMarker), IN_BATCH),
      seqMapper: (
          ((Iterable[IN], LedgerEndMarker), IN_BATCH),
          ((Iterable[IN], LedgerEndMarker), IN_BATCH),
      ) => (
          (Iterable[IN], LedgerEndMarker),
          IN_BATCH,
      ),
      batchingParallelism: Int,
      batcher: (
          ((Iterable[IN], LedgerEndMarker), IN_BATCH)
      ) => Future[((Iterable[IN], LedgerEndMarker), DB_BATCH)],
      ingestingParallelism: Int,
      ingester: (
          ((Iterable[IN], LedgerEndMarker), DB_BATCH)
      ) => Future[((Iterable[IN], LedgerEndMarker), DB_BATCH)],
      tailer: (
          ((Iterable[IN], LedgerEndMarker), DB_BATCH),
          ((Iterable[IN], LedgerEndMarker), DB_BATCH),
      ) => (
          (Iterable[IN], LedgerEndMarker),
          DB_BATCH,
      ),
      ingestTail: (
          ((Iterable[IN], LedgerEndMarker), DB_BATCH)
      ) => Future[(Iterable[IN], LedgerEndMarker)],
      updateInMemoryBuffersFlow: Flow[(Iterable[IN], LedgerEndMarker), Unit, NotUsed],
  )(source: Source[IN, NotUsed]): Source[Unit, NotUsed] = {
    val _ = batchWithinMillis
    // Stage 1: the stream coming from ReadService, involves deserialization and translation to Update-s
    source
      // Stage 2: Batching plus mapping to Database DTOs encapsulates all the CPU intensive computation of the ingestion. Executed in parallel.
      .groupedWithin(submissionBatchSize.toInt, FiniteDuration(1, "millis"))
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
      // Stage 6: Preparing data sequentially for throttled mutations in database (tracking the ledger-end, corresponding sequential event ids and latest-at-the-time configurations)
      .conflate(tailer)
      // Stage 7: Updating ledger-end and related data in database (this stage completion demarcates the consistent point-in-time)
      .mapAsync(1)(ingestTail)
      .via(updateInMemoryBuffersFlow)
  }
}
