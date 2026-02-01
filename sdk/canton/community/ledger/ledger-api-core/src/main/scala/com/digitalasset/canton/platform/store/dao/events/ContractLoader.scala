// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.resources.ResourceOwner
import com.daml.metrics.InstrumentedGraph
import com.daml.metrics.api.MetricHandle.Histogram
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.participant.state.index.ContractStateStatus.{
  Active,
  Archived,
  ExistingContractStatus,
}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.{BatchLoaderMetrics, LedgerApiServerMetrics}
import com.digitalasset.canton.platform.store.LedgerApiContractStore
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.interfaces.LedgerDaoContractsReader.{
  KeyAssigned,
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
  def contracts: Loader[(ContractId, Long), ExistingContractStatus]
  def keys: Loader[(GlobalKey, Long), KeyState]
}

object ContractLoader {

  private[events] def maxOffsetAndContextFromBatch[T](
      batch: Seq[((T, Long), LoggingContextWithTrace)],
      histogram: Histogram,
  ): (Long, LoggingContextWithTrace) = {
    val ((_, latestValidAtEventSeqId), usedLoggingContext) = batch
      .maxByOption(_._1._2)
      .getOrElse(
        throw new IllegalStateException("A batch should never be empty")
      )
    histogram.update(batch.size)(MetricsContext.Empty)
    (latestValidAtEventSeqId, usedLoggingContext)
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
      contractStore: LedgerApiContractStore,
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
    (ContractId, Long),
    ExistingContractStatus,
  ]] =
    ResourceOwner
      .forReleasable(() =>
        new PekkoStreamParallelBatchedLoader[
          (ContractId, Long),
          ExistingContractStatus,
        ](
          batchLoad = { batch =>
            val (latestValidAtEventSeqId, usedLoggingContext) = maxOffsetAndContextFromBatch(
              batch,
              metrics.index.db.activeContracts.batchSize,
            )
            val contractIds = batch.map(_._1._1)
            for {
              contractIdToInternalContractId <- contractStore
                .lookupBatchedInternalIds(contractIds)(usedLoggingContext.traceContext)
              internalContractIds = contractIds.flatMap(contractIdToInternalContractId.get)
              contractStatuses <- dbDispatcher
                .executeSql(metrics.index.db.lookupActiveContractsDbMetrics)(
                  contractStorageBackend.activeContracts(
                    internalContractIds = internalContractIds,
                    beforeEventSeqId = latestValidAtEventSeqId,
                  )
                )(usedLoggingContext)
            } yield batch.view.flatMap { case ((contractId, beforeEventSeqId), _) =>
              contractIdToInternalContractId
                .get(contractId)
                .flatMap(contractStatuses.get)
                .map {
                  case true => Active
                  case false => Archived
                }
                .map((contractId, beforeEventSeqId) -> _)
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
      contractStore: LedgerApiContractStore,
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
    (GlobalKey, Long),
    KeyState,
  ]] =
    ResourceOwner
      .forReleasable(() =>
        new PekkoStreamParallelBatchedLoader[
          (GlobalKey, Long),
          KeyState,
        ](
          batchLoad = { batch =>
            // we can use the latest offset as the API only requires us to not return a state older than the given offset
            val (latestValidAtEventSeqId, usedLoggingContext) =
              ContractLoader.maxOffsetAndContextFromBatch(
                batch,
                metrics.index.db.activeContracts.batchSize,
              )
            val contractKeys = batch.map(_._1._1)
            for {
              contractKeys <- dbDispatcher
                .executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
                  contractStorageBackend.keyStates(
                    keys = contractKeys,
                    validAtEventSeqId = latestValidAtEventSeqId,
                  )
                )(usedLoggingContext)
              internalContractIdToContractId <- contractStore
                .lookupBatchedContractIds(contractKeys.values)(
                  usedLoggingContext.traceContext
                )
            } yield batch.view.map { case ((key, eventSeqId), _) =>
              (key, eventSeqId) -> contractKeys
                .get(key)
                .flatMap(internalContractIdToContractId.get)
                .map(KeyAssigned.apply)
                .getOrElse(KeyUnassigned)
            }.toMap
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
      contractStore: LedgerApiContractStore,
      metrics: LedgerApiServerMetrics,
  )(
      keyWithValidAtEventSeqId: (GlobalKey, Long)
  )(implicit loggingContext: LoggingContextWithTrace, ec: ExecutionContext) = {
    val (key, validAtEventSeqId) = keyWithValidAtEventSeqId
    dbDispatcher
      .executeSql(metrics.index.db.lookupContractByKeyDbMetrics)(
        contractStorageBackend.keyState(
          key = key,
          validAtEventSeqId = validAtEventSeqId,
        )
      )(loggingContext)
      .flatMap {
        case Some(internalContractId) =>
          contractStore
            .lookupBatchedContractIds(List(internalContractId))(
              loggingContext.traceContext
            )
            .map(_.get(internalContractId).map(KeyAssigned.apply).getOrElse(KeyUnassigned))
        case None => Future.successful(KeyUnassigned)
      }
  }

  def create(
      participantContractStore: LedgerApiContractStore,
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
        participantContractStore,
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
            participantContractStore,
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
        override final val contracts: Loader[(ContractId, Long), ExistingContractStatus] =
          new Loader[(ContractId, Long), ExistingContractStatus] {
            override def load(key: (ContractId, Long))(implicit
                loggingContext: LoggingContextWithTrace
            ): Future[Option[ExistingContractStatus]] = contractsBatchLoader.load(key)
          }
        override final val keys: Loader[(GlobalKey, Long), KeyState] =
          contractKeysBatchLoader match {
            case Some(batchLoader) =>
              new Loader[(GlobalKey, Long), KeyState] {
                override def load(key: (GlobalKey, Long))(implicit
                    loggingContext: LoggingContextWithTrace
                ): Future[Option[KeyState]] = batchLoader.load(key)
              }
            case None =>
              new Loader[(GlobalKey, Long), KeyState] {
                override def load(key: (GlobalKey, Long))(implicit
                    loggingContext: LoggingContextWithTrace
                ): Future[Option[KeyState]] = fetchOneKey(
                  contractStorageBackend,
                  dbDispatcher,
                  participantContractStore,
                  metrics,
                )(key).map(Some(_))
              }
          }
      }
    }

  val dummyLoader = new ContractLoader {
    override final val contracts: Loader[(ContractId, Long), ExistingContractStatus] =
      new Loader[(ContractId, Long), ExistingContractStatus] {
        override def load(key: (ContractId, Long))(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[Option[ExistingContractStatus]] = Future.successful(None)
      }
    override final val keys: Loader[(GlobalKey, Long), KeyState] =
      new Loader[(GlobalKey, Long), KeyState] {
        override def load(key: (GlobalKey, Long))(implicit
            loggingContext: LoggingContextWithTrace
        ): Future[Option[KeyState]] = Future.successful(None)
      }

  }
}
