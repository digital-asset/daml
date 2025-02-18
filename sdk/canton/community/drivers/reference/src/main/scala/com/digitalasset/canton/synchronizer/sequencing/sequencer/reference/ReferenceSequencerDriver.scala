// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.reference

import com.digitalasset.canton.config.{ProcessingTimeout, QueryCostMonitoringConfig, StorageConfig}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.synchronizer.block.BlockFormat.{
  AcknowledgeTag,
  BatchTag,
  SendTag,
  blockOrdererBlockToRawLedgerBlock,
}
import com.digitalasset.canton.synchronizer.block.{
  BlockFormat,
  RawLedgerBlock,
  SequencerDriver,
  SequencerDriverHealthStatus,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.ReferenceSequencerDriver.{
  TimestampedRequest,
  batchRequests,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store.ReferenceBlockOrderingStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.store.v1.{
  TracedBatchedBlockOrderingRequests,
  TracedBlockOrderingRequest,
}
import com.digitalasset.canton.time.TimeProvider
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, PekkoUtil}
import com.digitalasset.canton.{RichGeneratedMessage, config}
import com.google.protobuf.ByteString
import io.grpc.ServerServiceDefinition
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class ReferenceSequencerDriver(
    store: ReferenceBlockOrderingStore,
    config: ReferenceSequencerDriver.Config[_ <: StorageConfig],
    timeProvider: TimeProvider,
    override val firstBlockHeight: Long,
    storage: Storage,
    closeable: AutoCloseable,
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
)(implicit
    executionContext: ExecutionContext,
    materializer: Materializer,
) extends SequencerDriver
    with NamedLogging
    with FlagCloseableAsync {

  private lazy val (sendQueue, done) = {
    implicit val traceContext: TraceContext = TraceContext.empty

    PekkoUtil.runSupervised(
      Source
        .queue[Traced[TimestampedRequest]](bufferSize = 100)
        .groupedWithin(n = config.maxBlockSize, d = config.maxBlockCutMillis.millis)
        .map { requests =>
          batchRequests(
            timeProvider.nowInMicrosecondsSinceEpoch,
            CantonTimestamp.MinValue, // this value is ignored, because it is only used by the BFT block orderer currently
            requests,
            TraceContext.empty,
          )
        }
        .map(req =>
          store.insertRequest(
            BlockFormat.OrderedRequest(req.microsecondsSinceEpoch, req.tag, req.body)
          )
        )
        .toMat(Sink.ignore)(Keep.both),
      errorLogMessagePrefix = "Fatally failed to handle state changes",
    )
  }

  override def subscribe()(implicit
      traceContext: TraceContext
  ): Source[RawLedgerBlock, KillSwitch] =
    ReferenceSequencerDriver
      .subscribe(firstBlockHeight)(store, config.pollInterval, logger)
      .map(blockOrdererBlockToRawLedgerBlock(logger))

  override def send(request: ByteString, submissionId: String, senderId: String)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    sendRequest(SendTag, request)

  override def acknowledge(acknowledgement: ByteString)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    sendRequest(AcknowledgeTag, acknowledgement)

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
    import TraceContext.Implicits.Empty.*
    Seq[AsyncOrSyncCloseable](
      SyncCloseable("sendQueue", sendQueue.complete()),
      AsyncCloseable("done", done, timeouts.closing),
      SyncCloseable("store", store.close()),
      SyncCloseable("storage", storage.close()),
      SyncCloseable("closeable", closeable.close()),
    )
  }

  override def adminServices: Seq[ServerServiceDefinition] = Seq.empty

  private def sendRequest(
      tag: String,
      body: ByteString,
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future.successful(storeRequest(timeProvider, tag, body))

  private[sequencer] def storeRequest(
      timeProvider: TimeProvider,
      tag: String,
      body: ByteString,
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      traceContext: TraceContext,
  ): Unit = {
    val microsecondsSinceEpoch = timeProvider.nowInMicrosecondsSinceEpoch
    sendQueue
      .offer(
        Traced(TimestampedRequest(tag, body, microsecondsSinceEpoch))
      ) match {
      case QueueOfferResult.Enqueued =>
        logger.debug(
          s"enqueued reference sequencer store request with tag $tag and sequencing time (ms since epoch) $microsecondsSinceEpoch"
        )
      case QueueOfferResult.Dropped =>
        // This should not happen
        ErrorUtil.internalError(
          new IllegalStateException(
            s"dropped reference store request with tag $tag and sequencing time (ms since epoch) $microsecondsSinceEpoch"
          )
        )
      case _: QueueCompletionResult =>
        logger.debug(
          s"won't enqueue reference sequencer request with tag $tag and sequencing time (ms since epoch) $microsecondsSinceEpoch because shutdown is in progress"
        )
    }
  }
}

object ReferenceSequencerDriver {

  /** Reference sequencer driver configuration
    * @param storage storage configuration for requests storage
    * @param pollInterval how often to poll for new blocks in blocks subscription
    */
  final case class Config[StorageConfigT <: StorageConfig](
      storage: StorageConfigT,
      pollInterval: config.NonNegativeFiniteDuration =
        config.NonNegativeFiniteDuration.ofMillis(100),
      logQueryCost: Option[QueryCostMonitoringConfig] = None,
      maxBlockSize: Int = 500,
      maxBlockCutMillis: Int = 1,
  )

  final case class TimestampedRequest(tag: String, body: ByteString, microsecondsSinceEpoch: Long)

  private def batchRequests(
      timestamp: Long,
      lastTopologyTimestamp: CantonTimestamp,
      requests: Seq[Traced[TimestampedRequest]],
      traceContext: TraceContext,
  ): BlockFormat.OrderedRequest = {
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
              request.microsecondsSinceEpoch,
            )
          },
          lastTopologyTimestamp.toMicros,
        )
        .checkedToByteString
    BlockFormat.OrderedRequest(timestamp, BatchTag, body)
  }

  def subscribe(fromHeight: Long)(
      store: ReferenceBlockOrderingStore,
      pollInterval: config.NonNegativeFiniteDuration,
      logger: TracedLogger,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Source[BlockFormat.Block, KillSwitch] = {
    logger.debug(
      s"Subscription started from height $fromHeight, current max height in DB is ${store.maxBlockHeight()}"
    )
    Source
      .tick(
        initialDelay = 0.milli,
        interval = pollInterval.underlying,
        (),
      )
      .viaMat(KillSwitches.single)(Keep.right)
      .scanAsync(
        fromHeight -> Seq[BlockFormat.Block]()
      ) { case ((nextFromHeight, _), _tick) =>
        ((for {
          newBlocks <-
            store.queryBlocks(nextFromHeight).map { timestampedBlocks =>
              val blocks = timestampedBlocks.map(_.block)
              if (logger.underlying.isDebugEnabled() && blocks.nonEmpty) {
                logger.debug(
                  s"New blocks (${blocks.length}) starting at height $nextFromHeight, specifically at ${blocks.map(_.blockHeight).mkString(",")}"
                )
              }
              blocks.lastOption.foreach { lastBlock =>
                val expectedLastBlockHeight = nextFromHeight + blocks.length - 1
                if (lastBlock.blockHeight != expectedLastBlockHeight) {
                  logger.warn(
                    s"Last block height was expected to be $expectedLastBlockHeight but was ${lastBlock.blockHeight}. " +
                      "This might point to a gap in queried blocks (visible under debug logging) and cause the BlockSequencer subscription to become stuck."
                  )
                }
              }
              blocks
            }
        } yield {
          // Setting the "new nextFromHeight" watermark block height based on the number of new blocks seen
          // assumes that store.queryBlocks returns consecutive blocks with "no gaps". See #13539.
          (nextFromHeight + newBlocks.size) -> newBlocks
        }).failOnShutdownToAbortException("ReferenceSequencerDriver.subscribe"))
      }
      .mapConcat(_._2)
  }
}
