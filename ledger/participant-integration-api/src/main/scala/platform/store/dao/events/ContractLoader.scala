// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.daml.error.{ContextualizedErrorLogger, DamlContextualizedErrorLogger}
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.{InstrumentedGraph, Metrics}
import com.daml.platform.indexer.parallel.BatchN
import com.daml.platform.store.backend.ContractStorageBackend
import com.daml.platform.store.dao.DbDispatcher

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

trait Loader[KEY, VALUE] {

  def load(key: KEY)(implicit loggingContext: LoggingContext): Future[Option[VALUE]]

}

class AkkaStreamParallelBatchedLoader[KEY, VALUE](
    batchLoad: Seq[(KEY, LoggingContext)] => Future[Map[KEY, VALUE]],
    createQueue: () => Source[(KEY, LoggingContext, Promise[Option[VALUE]]), BoundedSourceQueue[
      (KEY, LoggingContext, Promise[Option[VALUE]])
    ]],
    maxBatchSize: Int,
    parallelism: Int,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends Loader[KEY, VALUE] {
  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    DamlContextualizedErrorLogger.forClass(this.getClass)

  private val (queue, done) = createQueue() // TODO maybe set up autorestart?
    .via(
      BatchN(
        maxBatchSize = maxBatchSize,
        maxBatchCount = parallelism,
      )
    )
    .mapAsyncUnordered(parallelism) { batch =>
      batchLoad(
        batch.view.map { case (key, loggingContext, _) => key -> loggingContext }.toSeq
      ).transform {
        case Success(resultMap) =>
          batch.view.foreach { case (key, _, promise) =>
            promise.success(resultMap.get(key))
            ()
          }
          Success(())

        case Failure(t) =>
          batch.view.foreach { case (_, _, promise) =>
            promise.failure(
              LedgerApiErrors.InternalError
                .Generic("Contract lookup failed", Some(t))
                .asGrpcError
            )
            ()
          }
          Success(())
      }
    }
    .toMat(Sink.ignore)(Keep.both)
    .run()

  override def load(key: KEY)(implicit loggingContext: LoggingContext): Future[Option[VALUE]] = {
    val promise = Promise[Option[VALUE]]()
    queue.offer((key, loggingContext, promise)) match {
      case QueueOfferResult.Enqueued => promise.future

      case QueueOfferResult.Dropped =>
        Future.failed(
          LedgerApiErrors.ParticipantBackpressure
            .Rejection("Too many pending contract lookups")
            .asGrpcError
        )

      // these should never happen, if this service is closed in the right order
      case QueueOfferResult.QueueClosed => Future.failed(new IllegalStateException("Queue closed"))
      case QueueOfferResult.Failure(t) => Future.failed(new IllegalStateException(t.getMessage))
    }
  }

  def closeAsync(): Future[Unit] = {
    queue.complete()
    done.map(_ => ())
  }
}

trait ContractLoader extends Loader[(ContractId, Offset), ContractStorageBackend.RawContractState]

object ContractLoader {
  def create(
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
      maxQueueSize: Int,
      maxBatchSize: Int,
      parallelism: Int,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): ResourceOwner[ContractLoader] = {
    ResourceOwner
      .forReleasable(() =>
        new AkkaStreamParallelBatchedLoader[
          (ContractId, Offset),
          ContractStorageBackend.RawContractState,
        ](
          batchLoad = { batch =>
            val ((_, latestValidAtOffset), usedLoggingContext) = batch.maxBy(_._1._2)
            metrics.daml.index.db.activeContractLookupBatchSize
              .update(batch.size)(MetricsContext.Empty)
            dbDispatcher
              .executeSql(metrics.daml.index.db.lookupActiveContractsDbMetrics)(
                contractStorageBackend.contractStates(
                  contractIds = batch.map(_._1._1),
                  before = latestValidAtOffset,
                )
              )(usedLoggingContext)
              .map(results =>
                batch.view.flatMap { case ((contractId, offset), _) =>
                  results.get(contractId).map((contractId, offset) -> _).toList
                }.toMap
              )
          },
          createQueue = () =>
            InstrumentedGraph.queue(
              bufferSize = maxQueueSize,
              capacityCounter = metrics.daml.index.db.activeContractLookupBufferCapacity,
              lengthCounter = metrics.daml.index.db.activeContractLookupBufferLength,
              delayTimer = metrics.daml.index.db.activeContractLookupBufferDelay,
            ),
          maxBatchSize = maxBatchSize,
          parallelism = parallelism,
        )
      )(_.closeAsync())
      .map(loader =>
        new ContractLoader {
          override def load(key: (ContractId, Offset))(implicit
              loggingContext: LoggingContext
          ): Future[Option[ContractStorageBackend.RawContractState]] =
            loader.load(key)
        }
      )
  }
}
