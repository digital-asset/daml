// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

object BatchingParallelIngestionPipe {
  private type WithContext[IN, PAYLOAD] = ((Iterable[IN], LedgerEndMarker), PAYLOAD)
  private type Aggregate[O] = (O, O) => O

  def apply[IN, IN_BATCH, DB_BATCH](
      submissionBatchSize: Long,
      batchWithinMillis: Long,
      inputMappingParallelism: Int,
      inputMapper: Iterable[IN] => Future[WithContext[IN, IN_BATCH]],
      seqMapperZero: WithContext[IN, IN_BATCH],
      seqMapper: Aggregate[WithContext[IN, IN_BATCH]],
      batchingParallelism: Int,
      batcher: WithContext[IN, IN_BATCH] => Future[WithContext[IN, DB_BATCH]],
      ingestingParallelism: Int,
      ingester: WithContext[IN, DB_BATCH] => Future[WithContext[IN, DB_BATCH]],
      tailer: Aggregate[WithContext[IN, DB_BATCH]],
      ingestTail: WithContext[IN, DB_BATCH] => Future[(Iterable[IN], LedgerEndMarker)],
      updateInMemoryBuffersFlow: Flow[(Iterable[IN], LedgerEndMarker), Unit, NotUsed],
  )(source: Source[IN, NotUsed]): Source[Unit, NotUsed] = {
    val _ = batchWithinMillis
    // Stage 1: the stream coming from ReadService, involves deserialization and translation to Update-s
    source
      // Stage 2: Batching plus mapping to Database DTOs encapsulates all the CPU intensive computation of the ingestion. Executed in parallel.
      // On backpressure, pre-fill `inputMappingParallelism` batches ensuring even size distribution
      .via(batchN(submissionBatchSize.toInt, inputMappingParallelism))
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

  private def batchN[IN](
      maxBatchSize: Int,
      maxNumberOfBatches: Int,
  ): Flow[IN, ArrayBuffer[IN], NotUsed] =
    Flow[IN]
      .batch[Vector[ArrayBuffer[IN]]](
        (maxBatchSize * maxNumberOfBatches).toLong,
        in => Vector(newBatch(maxBatchSize, in)),
      ) { case (batches, in) =>
        val lastBatch = batches.last
        if (lastBatch.size < maxBatchSize) {
          lastBatch.addOne(in)
          batches
        } else
          batches :+ newBatch(maxBatchSize, in)
      }
      .flatMapConcat(v => Source.fromIterator(() => v.iterator))

  private def newBatch[IN](maxBatchSize: Int, newElement: IN) = {
    val newBatch = ArrayBuffer.empty[IN]
    newBatch.sizeHint(maxBatchSize)
    newBatch.addOne(newElement)
    newBatch
  }
}
