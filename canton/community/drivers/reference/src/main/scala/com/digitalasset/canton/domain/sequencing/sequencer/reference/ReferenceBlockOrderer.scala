// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config
import com.digitalasset.canton.config.{ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.BlockOrderingSequencer.BatchTag
import com.digitalasset.canton.domain.block.{
  BlockOrderer,
  SequencerDriverHealthStatus,
  TransactionSignature,
}
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore
import com.digitalasset.canton.domain.sequencing.sequencer.reference.store.v1.{
  TracedBatchedBlockOrderingRequests,
  TracedBlockOrderingRequest,
}
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, FlagCloseableAsync, SyncCloseable}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.time.TimeProvider
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.SimpleExecutionQueue
import com.google.protobuf.ByteString
import io.grpc.BindableService
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches}

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class ReferenceBlockOrderer(
    store: ReferenceBlockOrderingStore,
    pollInterval: config.NonNegativeFiniteDuration,
    timeProvider: TimeProvider,
    storage: Storage,
    closeable: AutoCloseable,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
)(implicit
    executionContext: ExecutionContext
) extends BlockOrderer
    with NamedLogging
    with FlagCloseableAsync {

  // this will help decrease the number of retries the db has to do due to id collisions
  private[sequencer] val sendQueue = new SimpleExecutionQueue(
    "reference-sequencer-send-queue",
    FutureSupervisor.Noop,
    ProcessingTimeout(),
    loggerFactory,
  )

  override def grpcServices: Seq[BindableService] = Seq()

  override def sendRequest(
      tag: String,
      body: ByteString,
      signature: Option[TransactionSignature] = None,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storeRequest(timeProvider, sendQueue, store, tag, body)

  override def subscribe(fromHeight: Long)(implicit
      traceContext: TraceContext
  ): Source[BlockOrderer.Block, KillSwitch] =
    Source
      .tick(
        initialDelay = 0.milli,
        interval = pollInterval.underlying,
        (),
      )
      .viaMat(KillSwitches.single)(Keep.right)
      .mapAsync(1)(_ => store.countBlocks().map(_ - 1L))
      .scanAsync(
        (fromHeight - 1L, Seq[BlockOrderer.Block]())
      ) { case ((lastHeight, _), currentHeight) =>
        for {
          newBlocks <-
            if (currentHeight > lastHeight) {
              store.queryBlocks(lastHeight + 1L).map { timestampedBlocks =>
                val blocks = timestampedBlocks.map(_.block)
                if (logger.underlying.isDebugEnabled()) {
                  logger.debug(
                    s"New blocks (${blocks.length}) at heights ${lastHeight + 1} to $currentHeight, specifically at ${blocks.map(_.blockHeight).mkString(",")}"
                  )
                }
                blocks.lastOption.foreach { lastBlock =>
                  if (lastBlock.blockHeight != lastHeight + blocks.length) {
                    logger.warn(
                      s"Last block height was expected to be ${lastHeight + blocks.length} but was ${lastBlock.blockHeight}. " +
                        "This might point to a gap in queried blocks (visible under debug logging) and cause the BlockSequencer subscription to become stuck."
                    )
                  }
                }
                blocks
              }
            } else {
              Future.successful(Seq.empty[BlockOrderer.Block])
            }
        } yield {
          // Setting the "new lastHeight" watermark block height based on the number of new blocks seen
          // assumes that store.queryBlocks returns consecutive blocks with "no gaps". See #13539.
          (lastHeight + newBlocks.size) -> newBlocks
        }
      }
      .mapConcat(_._2)

  override def health(implicit traceContext: TraceContext): Future[SequencerDriverHealthStatus] = {
    val isStorageActive = storage.isActive
    Future.successful(
      SequencerDriverHealthStatus(
        isActive = isStorageActive,
        description =
          if (isStorageActive) None else Some("Reference driver can't connect to database"),
      )
    )
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    Seq[AsyncOrSyncCloseable](
      SyncCloseable("sendQueue", sendQueue.close()),
      SyncCloseable("storage", storage.close()),
      SyncCloseable("closeable", closeable.close()),
    )
  }

  private[sequencer] def storeRequest(
      timeProvider: TimeProvider,
      sendQueue: SimpleExecutionQueue,
      store: ReferenceBlockOrderingStore,
      tag: String,
      body: ByteString,
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
  ): Future[Unit] = {
    val microsecondsSinceEpoch = timeProvider.nowInMicrosecondsSinceEpoch
    sendQueue
      .execute(
        store.insertRequest(
          BlockOrderer.OrderedRequest(microsecondsSinceEpoch, tag, body)
        ),
        s"send request at $microsecondsSinceEpoch",
      )
      .unwrap
      .map(_ =>
        logger.debug(
          s"Successfully executed a request sent at $microsecondsSinceEpoch with tag $tag"
        )
      )
  }
}

object ReferenceBlockOrderer {

  /** Reference sequencer driver configuration
    * @param storage storage configuration for requests storage
    * @param pollInterval how often to poll for new blocks in blocks subscription
    */
  final case class Config[StorageConfigT <: StorageConfig](
      storage: StorageConfigT,
      pollInterval: config.NonNegativeFiniteDuration =
        config.NonNegativeFiniteDuration.ofMillis(100),
  )

  final case class TimestampedRequest(tag: String, body: ByteString, timestamp: CantonTimestamp)

  private[sequencer] def storeBatch(
      blockHeight: Long,
      timestamp: CantonTimestamp,
      lastTopologyTimestamp: CantonTimestamp,
      sendQueue: SimpleExecutionQueue,
      store: ReferenceBlockOrderingStore,
      requests: Seq[Traced[TimestampedRequest]],
  )(implicit
      executionContext: ExecutionContext,
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
  ): Future[Unit] = {
    val batchTraceparent = traceContext.asW3CTraceContext.map(_.parent).getOrElse("")
    val body =
      TracedBatchedBlockOrderingRequests
        .of(
          batchTraceparent,
          requests.map { case traced @ Traced(request) =>
            val requestTraceparent =
              traced.traceContext.asW3CTraceContext.map(_.parent).getOrElse("")
            TracedBlockOrderingRequest(
              requestTraceparent,
              request.tag,
              request.body,
              request.timestamp.toMicros,
            )
          },
          lastTopologyTimestamp.toMicros,
        )
        .toByteString

    sendQueue
      .execute(
        store.insertRequestWithHeight(
          blockHeight,
          BlockOrderer.OrderedRequest(timestamp.toMicros, BatchTag, body),
        ),
        s"send request at $timestamp",
      )
      .unwrap
      .map(_ => ())
  }
}
