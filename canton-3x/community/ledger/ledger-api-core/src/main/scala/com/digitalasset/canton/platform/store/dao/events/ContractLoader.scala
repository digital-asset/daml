// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.value.Value.ContractId
import com.daml.metrics.InstrumentedGraph
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend
import com.digitalasset.canton.platform.store.backend.ContractStorageBackend.{
  RawArchivedContract,
  RawContractState,
  RawCreatedContract,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.util.PekkoUtil.syntax.*
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
            Success(())

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
            Success(())
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

trait ContractLoader extends Loader[(ContractId, Offset), RawContractState]

object ContractLoader {
  def create(
      contractStorageBackend: ContractStorageBackend,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
      maxQueueSize: Int,
      maxBatchSize: Int,
      parallelism: Int,
      multiDomainEnabled: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
  ): ResourceOwner[ContractLoader] = {
    ResourceOwner
      .forReleasable(() =>
        new PekkoStreamParallelBatchedLoader[
          (ContractId, Offset),
          RawContractState,
        ](
          batchLoad = { batch =>
            val ((_, latestValidAtOffset), usedLoggingContext) = batch
              .maxByOption(_._1._2)
              .getOrElse(
                throw new IllegalStateException("A batch should never be empty")
              )
            metrics.daml.index.db.activeContractLookupBatchSize
              .update(batch.size)(MetricsContext.Empty)
            val contractIds = batch.map(_._1._1)
            val archivedContractsF =
              dbDispatcher
                .executeSql(metrics.daml.index.db.lookupArchivedContractsDbMetrics)(
                  contractStorageBackend.archivedContracts(
                    contractIds = contractIds,
                    before = latestValidAtOffset,
                  )
                )(usedLoggingContext)
            val createdContractsF =
              dbDispatcher
                .executeSql(metrics.daml.index.db.lookupCreatedContractsDbMetrics)(
                  contractStorageBackend.createdContracts(
                    contractIds = contractIds,
                    before = latestValidAtOffset,
                  )
                )(usedLoggingContext)
            def additionalContractsF(
                archivedContracts: Map[ContractId, RawArchivedContract],
                createdContracts: Map[ContractId, RawCreatedContract],
            ): Future[Map[ContractId, RawCreatedContract]] =
              if (multiDomainEnabled) {
                val notFoundContractIds = contractIds.view
                  .filterNot(archivedContracts.contains)
                  .filterNot(createdContracts.contains)
                  .toSeq
                if (notFoundContractIds.isEmpty) Future.successful(Map.empty)
                else
                  dbDispatcher.executeSql(metrics.daml.index.db.lookupAssignedContractsDbMetrics)(
                    // The latestValidAtOffset is not used here as an upper bound for the lookup,
                    // since the ContractStateCache only tracks creation and archival, therefore the
                    // index is not moving ahead in case of assignment. This in corner cases would mean
                    // that the index is behind of some assignments.
                    // Instead the query is constrained by the ledgerEndCache.
                    contractStorageBackend.assignedContracts(notFoundContractIds)
                  )(usedLoggingContext)
              } else Future.successful(Map.empty)
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
          createQueue = () =>
            InstrumentedGraph.queue(
              bufferSize = maxQueueSize,
              capacityCounter = metrics.daml.index.db.activeContractLookupBufferCapacity,
              lengthCounter = metrics.daml.index.db.activeContractLookupBufferLength,
              delayTimer = metrics.daml.index.db.activeContractLookupBufferDelay,
            ),
          maxBatchSize = maxBatchSize,
          parallelism = parallelism,
          loggerFactory = loggerFactory,
        )
      )(_.closeAsync())
      .map(loader =>
        new ContractLoader {
          override def load(key: (ContractId, Offset))(implicit
              loggingContext: LoggingContextWithTrace
          ): Future[Option[RawContractState]] =
            loader.load(key)
        }
      )
  }

  val dummyLoader = new ContractLoader {
    override def load(key: (ContractId, Offset))(implicit
        loggingContext: LoggingContextWithTrace
    ): Future[Option[RawContractState]] = Future.successful(None)
  }
}
