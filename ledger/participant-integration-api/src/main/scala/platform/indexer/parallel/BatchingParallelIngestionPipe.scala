// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{Offset, Update}
import com.daml.platform.store.appendonlydao.JdbcLedgerDao

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object BatchingParallelIngestionPipe {

  def apply[IN, RUNBATCH](
      submissionBatchSize: Long,
      batchWithinMillis: Long,
      inputMappingParallelism: Int,
      inputMapper: Iterable[IN] => Future[RUNBATCH],
      seqMapperZero: RUNBATCH,
      seqMapper: (RUNBATCH, RUNBATCH) => RUNBATCH,
      ingestingParallelism: Int,
      ingester: RUNBATCH => Future[RUNBATCH],
      tailer: (RUNBATCH, RUNBATCH) => RUNBATCH,
      tailingRateLimitPerSecond: Int,
      ingestTail: RUNBATCH => Future[RUNBATCH],
  )(source: Source[IN, NotUsed]): Source[Unit, NotUsed] =
    // Stage 1: the stream coming from ReadService, involves deserialization and translation to Update-s
    source
      // Stage 2: Batching plus mapping to Database DTOs encapsulates all the CPU intensive computation of the ingestion. Executed in parallel.
      .groupedWithin(
        submissionBatchSize.toInt,
        FiniteDuration(batchWithinMillis, "millis"),
      ) // TODO append-only: .batch adds no latency to the pipe, but batch and mapAsync combination leads to dirac impulses in the forming batch sizes, which leads practically single threaded ingestion throughput at this stage.
      .mapAsync(inputMappingParallelism)(inputMapper)
      // Stage 3: Encapsulates sequential/stateful computation (generation of sequential IDs for events)
      .scan(seqMapperZero)(seqMapper)
      .drop(1) // remove the zero element from the beginning of the stream
      // Stage 4: Inserting data into the database. Almost no CPU load here, threads are executing SQL commands over JDBC, and waiting for the result. This defines the parallelism on the SQL database side, same amount of PostgreSQL Backend processes will do the ingestion work.
      .async
      .mapAsync(ingestingParallelism)(ingester)
      // Stage 5: Preparing data sequentially for throttled mutations in database (tracking the ledger-end, corresponding sequential event ids and latest-at-the-time configurations)
      .conflate(tailer)
      .throttle(tailingRateLimitPerSecond, FiniteDuration(1, "seconds"))
      // Stage 6: Updating ledger-end and related data in database (this stage completion demarcates the consistent point-in-time)
      .mapAsync(1)(ingestTail)
      .map(_ =>
        ()
      ) // TODO append-only: linking to consumers that depend on the moving ledger end, such as in-memory fan-out

}

case class RunningDBBatch(
    lastOffset: Offset,
    lastSeqEventId: Long,
    lastConfig: Option[Array[Byte]],
    batch: RawDBBatchPostgreSQLV1,
    batchSize: Int,
    averageStartTime: Long,
)

object RunningDBBatch {

  def inputMapper(
      toDbDto: Offset => Update => Iterator[DBDTOV1],
      averageStartTime: Long,
  )(input: Iterable[(Offset, Update)]): RunningDBBatch = {
    val batchBuilder = RawDBBatchPostgreSQLV1.Builder()
    var lastOffset: Offset = null
    var lastConfiguration: Array[Byte] = null
    input.foreach { case (offset, update) =>
      val dbDtos = toDbDto(offset)(update)
      lastOffset = offset
      dbDtos.foreach { dbDto =>
        dbDto match {
          case c: DBDTOV1.ConfigurationEntry if c.typ == JdbcLedgerDao.acceptType =>
            lastConfiguration = c.configuration
          case _ =>
            ()
        }
        batchBuilder.add(dbDto)
      }
    }
    val batch = batchBuilder.build()
    RunningDBBatch(
      lastOffset = lastOffset,
      lastSeqEventId = 0L, // will be populated later
      lastConfig = Option(lastConfiguration),
      batch = batch,
      batchSize = input.size,
      averageStartTime = averageStartTime,
    )
  }

  def seqMapperZero(initialSeqEventId: Long): RunningDBBatch =
    RunningDBBatch(
      lastOffset = null,
      lastSeqEventId = initialSeqEventId,
      lastConfig = None,
      batch = nullBatch,
      batchSize = 0,
      averageStartTime = 0,
    )

  def seqMapper(prev: RunningDBBatch, curr: RunningDBBatch): RunningDBBatch = {
    val idsUsed = curr.batch.offsetSequentialEventIds(prev.lastSeqEventId + 1)
    curr.copy(lastSeqEventId = prev.lastSeqEventId + idsUsed)
  }

  private val nullBatch = RawDBBatchPostgreSQLV1.Builder().build()

  def tailer(prev: RunningDBBatch, curr: RunningDBBatch): RunningDBBatch =
    RunningDBBatch(
      lastOffset = curr.lastOffset,
      lastSeqEventId = curr.lastSeqEventId,
      lastConfig = curr.lastConfig.orElse(prev.lastConfig),
      batch = nullBatch,
      batchSize = 0,
      averageStartTime = 0,
    )
}
