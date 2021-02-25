package com.daml.platform.indexer.poc

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import com.daml.ledger.participant.state.v1.{Offset, ParticipantId, ReadService, Update}
import com.daml.ledger.resources.{Resource, ResourceOwner}
import com.daml.platform.indexer.poc.AsyncSupport._
import com.daml.platform.indexer.poc.PerfSupport._
import com.daml.platform.indexer.poc.StaticMetrics._
import com.daml.platform.indexer.{Indexer, SubscriptionIndexFeedHandle}
import com.daml.platform.store.dao.events.LfValueTranslation

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object PoCIndexerFactory {

  // TODO migrate code for mutable initialisation
  def apply(jdbcUrl: String,
            participantId: ParticipantId,
            translation: LfValueTranslation,
            mat: Materializer,
            inputMappingParallelism: Int,
            ingestionParallelism: Int,
            submissionBatchSize: Long,
            tailingRateLimitPerSecond: Int,
            batchWithinMillis: Long,
            runStageUntil: Int): ResourceOwner[Indexer] = {
    for {
      inputMapperExecutor <- asyncPool(inputMappingParallelism)
      postgresDaoPool <- asyncResourcePool(() => JDBCPostgresDAO(jdbcUrl), size = ingestionParallelism + 1)
    } yield {
      val ingest: Long => Source[(Offset, Update), NotUsed] => Source[Unit, NotUsed] =
        initialSeqId => source => BatchingParallelIngestionPipe(
          submissionBatchSize = submissionBatchSize,
          batchWithinMillis = batchWithinMillis,
          inputMappingParallelism = inputMappingParallelism,
          inputMapper = inputMapperExecutor.execute(
            (input: Iterable[(Offset, Update)]) => withMetrics(mappingCPU) {
              val result = RunningDBBatch.inputMapper(UpdateToDBDTOV1(
                participantId = participantId,
                translation = translation
              ))(input)
              batchCounter.add(input.size.toLong)
              result
            }
          ),
          seqMapperZero = RunningDBBatch.seqMapperZero(initialSeqId),
          seqMapper = (prev: RunningDBBatch, curr: RunningDBBatch) => withMetrics(seqMappingCPU) {
            RunningDBBatch.seqMapper(prev, curr)
          },
          ingestingParallelism = ingestionParallelism,
          ingester = postgresDaoPool.execute[RunningDBBatch, RunningDBBatch](
            (batch: RunningDBBatch, dao: PostgresDAO) => withMetrics(ingestionCPU, dbCallHistrogram) {
              dao.insertBatch(batch.batch)
              batch
            }
          ),
          tailer = RunningDBBatch.tailer,
          tailingRateLimitPerSecond = tailingRateLimitPerSecond,
          ingestTail = postgresDaoPool.execute[RunningDBBatch, RunningDBBatch](
            (batch: RunningDBBatch, dao: PostgresDAO) => {
              dao.updateParams(
                ledgerEnd = batch.lastOffset,
                eventSeqId = batch.lastSeqEventId,
                configuration = batch.lastConfig
              )
              batch
            }
          ),
          runStageUntil = runStageUntil
        )(source).map(_ => ())

      def subscribe(readService: ReadService): Future[Source[Unit, NotUsed]] =
        postgresDaoPool.execute(_.initialize).map {
          case (offset, eventSeqId) =>
            ingest(eventSeqId.getOrElse(0L))(readService.stateUpdates(beginAfter = offset))
        }(scala.concurrent.ExecutionContext.global)

      indexerFluffAround(subscribe)(mat)
    }
  }

  def indexerFluffAround(ingestionPipeOn: ReadService => Future[Source[Unit, NotUsed]])
                        (implicit mat: Materializer): Indexer =
    readService => ResourceOwner {
      implicit context =>
        implicit val ec: ExecutionContext = context.executionContext
        Resource {
          ingestionPipeOn(readService).map {
            pipe =>
              val (killSwitch, completionFuture) = pipe
                .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
                .toMat(Sink.ignore)(Keep.both)
                .run()
              new SubscriptionIndexFeedHandle(killSwitch, completionFuture.map(_ => ()))
          }
        } {
          handle =>
            handle.killSwitch.shutdown()
            handle.completed.recover {
              case NonFatal(_) => ()
            }
        }
    }
}
