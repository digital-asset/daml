// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.InstrumentedGraph
import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.{BatchLoaderMetrics, LedgerApiServerMetrics}
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.{
  RawArchivedContract,
  RawContractState,
  RawCreatedContract,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyState,
  KeyUnassigned,
}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.TryUtil
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value.ContractId
import io.grpc.{Metadata, StatusRuntimeException}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{BoundedSourceQueue, Materializer, QueueOfferResult}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

trait Loader[KEY, VALUE] {

  def load(key: KEY)(implicit loggingContext: LoggingContextWithTrace): Future[Option[VALUE]]

}

class PekkoStreamParallelBatchedLoader[KEY, VALUE](
    batchLoad: Seq[(KEY, LoggingContextWithTrace)] => Future[Map[KEY, VALUE]],
    createQueue: () => Source[
      (KEY, LoggingContextWithTrace, Promise[Option[VALUE]]),
      BoundedSourceQueue[
        (KEY, LoggingContextWithTrace, Promise[Option[VALUE]])
      ],
    ],
    maxBatchSize: Int,
    parallelism: Int,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends Loader[KEY, VALUE]
    with NamedLogging {

  private val (queue, done) = createQueue()
    .batchN(
      maxBatchSize = maxBatchSize,
      maxBatchCount = parallelism,
    )
    .mapAsyncUnordered(parallelism) { batch =>
      Future
        .delegate(
          batchLoad(
            batch.view.map { case (key, loggingContext, _) => key -> loggingContext }.toSeq
          )
        )
        .transform {
          case Success(resultMap) =>
            batch.view.foreach { case (key, _, promise) =>
              promise.success(resultMap.get(key))
              ()
            }
            TryUtil.unit

          case Failure(t) =>
            batch.view.foreach { case (_, _, promise) =>
              promise.failure(
                t match {
                  case s: StatusRuntimeException =>
                    // creates a new array under the hood, which prevents un-synchronized concurrent changes in the gRPC serving layer
                    val newMetadata = new Metadata()
                    newMetadata.merge(s.getTrailers)
                    new StatusRuntimeException(s.getStatus, newMetadata)
                  case other => other
                }
              )
              ()
            }
            TryUtil.unit
        }
    }
    .toMat(Sink.ignore)(Keep.both)
    .run()

  override def load(
      key: KEY
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[VALUE]] = {
    val promise = Promise[Option[VALUE]]()
    queue.offer((key, loggingContext, promise)) match {
      case QueueOfferResult.Enqueued => promise.future

      case QueueOfferResult.Dropped =>
        Future.failed(
          LedgerApiErrors.ParticipantBackpressure
            .Rejection("Too many pending contract lookups")(
              ErrorLoggingContext(logger, loggingContext)
            )
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

/** Efficient cross-request batching contract loader
  *
  * Note that both loaders operate on an identifier -> offset basis. The given offset of a request
  * serves as a lower bound for the states. The states can be newer, but not older. We still need to
  * have an upper bound of the requests as we don't want to read dirty states (due to parallel
  * insertion).
  */
trait ContractLoader {
  def contracts: Loader[(ContractId, Offset), RawContractState]
  def keys: Loader[(GlobalKey, Offset), KeyState]
}

object ContractLoader {

  private[events] def maxOffsetAndContextFromBatch[T](
      batch: Seq[((T, Offset), LoggingContextWithTrace)],
      histogram: Histogram,
  ): (Offset, LoggingContextWithTrace) = {
    val ((_, latestValidAtOffset), usedLoggingContext) = batch
      .maxByOption(_._1._2)
      .getOrElse(
        throw new IllegalStateException("A batch should never be empty")
      )
    histogram.update(batch.size)(MetricsContext.Empty)
    (latestValidAtOffset, usedLoggingContext)
  }

  private[events] def createQueue[K, V](maxQueueSize: Int, metrics: BatchLoaderMetrics)(implicit
      materializer: Materializer
  ): Source[(K, LoggingContextWithTrace, Promise[Option[V]]), BoundedSourceQueue[
    (K, LoggingContextWithTrace, Promise[Option[V]])
  ]] =
    InstrumentedGraph.queue(
      bufferSize = maxQueueSize,
      capacityCounter = metrics.bufferCapacity,
      lengthCounter = metrics.bufferLength,
      delayTimer = metrics.bufferDelay,
    )

  private def createContractBatchLoader(
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      maxQueueSize: Int,
      maxBatchSize: Int,
      parallelism: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): ResourceOwner[PekkoStreamParallelBatchedLoader[
    (ContractId, Offset),
    RawContractState,
  ]] =
    ResourceOwner
      .forReleasable(() =>
        new PekkoStreamParallelBatchedLoader[
          (ContractId, Offset),
          RawContractState,
        ](
          batchLoad = { batch =>
            val (latestValidAtOffset, usedLoggingContext) = maxOffsetAndContextFromBatch(
              batch,
              metrics.index.db.activeContracts.batchSize,
            )
            val contractIds = batch.map(_._1._1)
            val archivedContractsF =
              dbDispatcher
                .executeSql(metrics.index.db.lookupArchivedContractsDbMetrics)(
                  contractStorageBackend.archivedContracts(
                    contractIds = contractIds,
                    before = latestValidAtOffset,
                  )
                )(usedLoggingContext)
            val createdContractsF =
              dbDispatcher
                .executeSql(metrics.index.db.lookupCreatedContractsDbMetrics)(
                  contractStorageBackend.createdContracts(
                    contractIds = contractIds,
                    before = latestValidAtOffset,
                  )
                )(usedLoggingContext)
            def additionalContractsF(
                archivedContracts: Map[ContractId, RawArchivedContract],
                createdContracts: Map[ContractId, RawCreatedContract],
            ): Future[Map[ContractId, RawCreatedContract]] = {
              val notFoundContractIds = contractIds.view
                .filterNot(archivedContracts.contains)
                .filterNot(createdContracts.contains)
                .toSeq
              if (notFoundContractIds.isEmpty) Future.successful(Map.empty)
              else
                dbDispatcher.executeSql(metrics.index.db.lookupAssignedContractsDbMetrics)(
                  contractStorageBackend.assignedContracts(
                    contractIds = notFoundContractIds,
                    before = latestValidAtOffset,
                  )
                )(usedLoggingContext)
            }
            for {
              archivedContracts <- archivedContractsF
              createdContracts <- createdContractsF
              additionalContracts <- additionalContractsF(
                archivedContracts = archivedContracts,
                createdContracts = createdContracts,
              )
            } yield batch.view.flatMap { case ((contractId, offset), _) =>
              archivedContracts
                .get(contractId)
                .orElse(createdContracts.get(contractId): Option[RawContractState])
                .orElse(additionalContracts.get(contractId): Option[RawContractState])
                .map((contractId, offset) -> _)
                .toList
            }.toMap
          },
          createQueue =
            () => ContractLoader.createQueue(maxQueueSize, metrics.index.db.activeContracts),
          maxBatchSize = maxBatchSize,
          parallelism = parallelism,
          loggerFactory = loggerFactory,
        )
      )(_.closeAsync())

  private def createContractKeyBatchLoader(
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      maxQueueSize: Int,
      maxBatchSize: Int,
      parallelism: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): ResourceOwner[PekkoStreamParallelBatchedLoader[
    (GlobalKey, Offset),
    KeyState,
  ]] =
    ResourceOwner
      .forReleasable(() =>
        new PekkoStreamParallelBatchedLoader[
          (GlobalKey, Offset),
          KeyState,
        ](
          batchLoad = { batch =>
            // we can use the latest offset as the API only requires us to not return a state older than the given offset
            val (latestValidAtOffset, usedLoggingContext) =
              ContractLoader.maxOffsetAndContextFromBatch(
                batch,
                metrics.index.db.activeContracts.batchSize,
              )
            val contractKeys = batch.map(_._1._1)
            val contractKeysF =
              dbDispatcher
                .executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
                  contractStorageBackend.keyStates(
                    keys = contractKeys,
                    validAt = latestValidAtOffset,
                  )
                )(usedLoggingContext)

            contractKeysF.map { keys =>
              batch.view.map { case (key, _offset) =>
                (key) -> keys
                  .getOrElse(
                    key._1, {
                      loggerFactory
                        .getLogger(getClass)
                        .error(
                          s"Key is absent (not even unassigned) in contract key lookup at the given offset: $key"
                        )
                      KeyUnassigned
                    },
                  )
              }.toMap
            }
          },
          createQueue =
            () => ContractLoader.createQueue(maxQueueSize, metrics.index.db.activeContracts),
          maxBatchSize = maxBatchSize,
          parallelism = parallelism,
          loggerFactory = loggerFactory,
        )
      )(_.closeAsync())

  private def fetchOneKey(
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
  )(
      keyWithOffset: (GlobalKey, Offset)
  )(implicit loggingContext: LoggingContextWithTrace) = {
    val (key, offset) = keyWithOffset
    dbDispatcher
      .executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
        contractStorageBackend.keyState(
          key = key,
          validAt = offset,
        )
      )(loggingContext)
  }

  def create(
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      metrics: LedgerApiServerMetrics,
      maxQueueSize: Int,
      maxBatchSize: Int,
      parallelism: Int,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): ResourceOwner[ContractLoader] =
    for {
      contractsBatchLoader <- createContractBatchLoader(
        contractStorageBackend,
        dbDispatcher,
        metrics,
        maxQueueSize,
        maxBatchSize,
        parallelism,
        loggerFactory,
      )
      contractKeysBatchLoader <-
        if (contractStorageBackend.supportsBatchKeyStateLookups)
          createContractKeyBatchLoader(
            contractStorageBackend,
            dbDispatcher,
            metrics,
            maxQueueSize,
            maxBatchSize,
            parallelism,
            loggerFactory,
          ).map(Some(_))
        else ResourceOwner.successful(None)
    } yield {
      new ContractLoader {
        override final val contracts: Loader[(ContractId, Offset), RawContractState] =
          new Loader[(ContractId, Offset), RawContractState] {
            override def load(key: (ContractId, Offset))(implicit
                loggingContext: LoggingContextWithTrace
            ): Future[Option[RawContractState]] = contractsBatchLoader.load(key)
          }
        override final val keys: Loader[(GlobalKey, Offset), KeyState] =
          contractKeysBatchLoader match {
            case Some(batchLoader) =>
              new Loader[(GlobalKey, Offset), KeyState] {
                override def load(key: (GlobalKey, Offset))(implicit
                    loggingContext: LoggingContextWithTrace
                ): Future[Option[KeyState]] = batchLoader.load(key)
              }
            case None =>
              new Loader[(GlobalKey, Offset), KeyState] {
                override def load(key: (GlobalKey, Offset))(implicit
                    loggingContext: LoggingContextWithTrace
                ): Future[Option[KeyState]] =
                  fetchOneKey(contractStorageBackend, dbDispatcher, metrics)(key).map(Some(_))
              }
          }
      }
    }

  val dummyLoader = new ContractLoader {
    override final val contracts: Loader[(ContractId, Offset), RawContractState] =
      new Loader[(ContractId, Offset), RawContractState] {
        override def load(key: (ContractId, Offset))(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[Option[RawContractState]] = Future.successful(None)
      }
    override final val keys: Loader[(GlobalKey, Offset), KeyState] =
      new Loader[(GlobalKey, Offset), KeyState] {
        override def load(key: (GlobalKey, Offset))(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[Option[KeyState]] = Future.successful(None)
      }

  }
}
