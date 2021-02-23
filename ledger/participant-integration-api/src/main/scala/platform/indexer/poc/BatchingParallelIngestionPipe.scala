package com.daml.platform.indexer.poc

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{Offset, Update}
import com.daml.platform.store.dao.JdbcLedgerDao

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object BatchingParallelIngestionPipe {

  def apply[IN, RUNBATCH](submissionBatchSize: Long,
                          batchWithinMillis: Long,
                          inputMappingParallelism: Int,
                          inputMapper: Iterable[IN] => Future[RUNBATCH],
                          seqMapperZero: RUNBATCH,
                          seqMapper: (RUNBATCH, RUNBATCH) => RUNBATCH,
                          ingestingParallelism: Int,
                          ingester: RUNBATCH => Future[RUNBATCH],
                          tailer: (RUNBATCH, RUNBATCH) => RUNBATCH,
                          tailingRateLimitPerSecond: Int,
                          ingestTail: RUNBATCH => Future[RUNBATCH])
                         (source: Source[IN, NotUsed]): Source[Unit, NotUsed] = {

    source
      .groupedWithin(submissionBatchSize.toInt, FiniteDuration(batchWithinMillis, "millis")) // TODO .batch adds no latency to the pipe, but batch and mapAsync combination leads to dirac impulses in the forming batch sizes, which leads practically single threaded ingestion throughput at this stage.
      .mapAsync(inputMappingParallelism)(inputMapper)
      .scan(seqMapperZero)(seqMapper)
      .drop(1) // remove the zero element from the beginning of the stream
      .async
      .mapAsync(ingestingParallelism)(ingester)
      .conflate(tailer)
      .throttle(tailingRateLimitPerSecond, FiniteDuration(1, "seconds"))
      .mapAsync(1)(ingestTail)
      .map(_ => ()) // TODO what should this out? linking to in-mem fan-out?
  }

}

case class RunningDBBatch(lastOffset: Offset,
                          lastSeqEventId: Long,
                          lastConfig: Option[Array[Byte]],
                          batch: RawDBBatchPostgreSQLV1)

object RunningDBBatch {

  def inputMapper(toDbDto: Offset => Update => Iterator[DBDTOV1])
                 (input: Iterable[(Offset, Update)]): RunningDBBatch = {
    val batchBuilder = RawDBBatchPostgreSQLV1.Builder()
    var lastOffset: Offset = null
    var lastConfiguration: Array[Byte] = null
    input.foreach {
      case (offset, update) =>
        val dbDtos = toDbDto(offset)(update)
        lastOffset = offset
        dbDtos.foreach {
          dbDto =>
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
      batch = batch
    )
  }

  def seqMapperZero(initialSeqEventId: Long): RunningDBBatch =
    RunningDBBatch(
      lastOffset = null,
      lastSeqEventId = initialSeqEventId,
      lastConfig = None,
      batch = nullBatch
    )

  def seqMapper(prev: RunningDBBatch, curr: RunningDBBatch): RunningDBBatch = {
    var seqEventId = prev.lastSeqEventId
    curr.batch.eventsBatch.foreach {
      eventsBatch =>
        eventsBatch.event_sequential_id.indices.foreach {
          i =>
            seqEventId += 1
            eventsBatch.event_sequential_id(i) = seqEventId
        }
    }
    curr.copy(lastSeqEventId = seqEventId)
  }

  private val nullBatch = RawDBBatchPostgreSQLV1.Builder().build()

  def tailer(prev: RunningDBBatch, curr: RunningDBBatch): RunningDBBatch =
    RunningDBBatch(
      lastOffset = curr.lastOffset,
      lastSeqEventId = curr.lastSeqEventId,
      lastConfig = curr.lastConfig.orElse(prev.lastConfig),
      batch = nullBatch
    )
}