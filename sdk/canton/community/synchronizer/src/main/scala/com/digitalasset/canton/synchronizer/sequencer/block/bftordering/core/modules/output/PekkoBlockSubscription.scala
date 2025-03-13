// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  BlockSubscription,
  Env,
}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.ActorAttributes.streamSubscriptionTimeout
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{
  KillSwitch,
  KillSwitches,
  Materializer,
  OverflowStrategy,
  QueueOfferResult,
  StreamSubscriptionTimeoutTerminationMode,
}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.{Failure, Success}

import PekkoBlockSubscription.BlockQueueBufferSize

class PekkoBlockSubscription[E <: Env[E]](
    initialHeight: BlockNumber,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(abort: String => Nothing)(implicit
    executionContext: ExecutionContext,
    materializer: Materializer,
) extends BlockSubscription
    with NamedLogging
    with FlagCloseableAsync {

  private lazy val (queue, source) = {
    val attributes = streamSubscriptionTimeout(
      0.milli, // this value won't be used
      StreamSubscriptionTimeoutTerminationMode.noop, // instead of .cancel
    )
    val queueSource =
      Source.queue[BlockFormat.Block](BlockQueueBufferSize, OverflowStrategy.backpressure)
    // Normally we'd simply call queueSource.preMaterialize() in order to materialize the queue from here.
    // We need to do that because we don't have access to the materialized values of the stream that uses
    // the source returned by subscription() but we need to have access to the queue that gets materialized
    // from the Source.queue in order to push the blocks that we want to serve.
    // However, we're explicitly spelling out the code from preMaterialize() here because there is a small
    // change that needs to be done, which is the addition of the custom attributes such that the stream
    // won't fail with a StreamDetachedException after a timeout (which is by default 5 seconds) from the time
    // the source gets created to the time it starts getting used.
    val (mat, pub) = materializer.materialize(
      queueSource.toMat(Sink.asPublisher(fanout = true))(Keep.both).addAttributes(attributes)
    )
    (mat, Source.fromPublisher(pub))
  }

  override def subscription(): Source[BlockFormat.Block, KillSwitch] =
    source
      .statefulMapConcat { () =>
        val blocksPeanoQueue = new PeanoQueue[BlockNumber, BlockFormat.Block](initialHeight)(abort)
        block => {
          val blockHeight = block.blockHeight
          logger.debug(
            s"Inserting block $blockHeight into subscription Peano queue (head=${blocksPeanoQueue.head})"
          )(TraceContext.empty)
          blocksPeanoQueue.insert(BlockNumber(blockHeight), block)
          blocksPeanoQueue.pollAvailable()
        }
      }
      .viaMat(KillSwitches.single)(Keep.right)

  override def receiveBlock(
      block: BlockFormat.Block
  )(implicit traceContext: TraceContext): Unit =
    // don't add new messages to queue if we are closing the queue, or we get a StreamDetached exception
    performUnlessClosingF("enqueue block") {
      logger.debug(s"Received block ${block.blockHeight}")
      queue.offer(block)
    }.onShutdown(QueueOfferResult.Enqueued).onComplete {
      case Success(value) =>
        value match {
          case QueueOfferResult.Enqueued =>
            logger.debug(
              s"Block w/ height=${block.blockHeight} successfully enqueued"
            )
          case QueueOfferResult.Dropped =>
            logger.warn(
              s"Dropped block w/ height=${block.blockHeight}"
            )
          case QueueOfferResult.Failure(cause) =>
            logger.warn(
              s"Block w/ height=${block.blockHeight} raised exception",
              cause,
            )
          case QueueOfferResult.QueueClosed =>
            logger.warn(
              s"Block w/ height=${block.blockHeight} failed to queue as the queue closed"
            )
        }
      case Failure(exception) =>
        performUnlessClosing("error enqueuing block")(
          // if this happens when we're not closing, it is most likely because the stream itself was closed by the BlockSequencer
          logger.debug(
            s"Failure to add OutputBlock w/ height=${block.blockHeight} to block queue. Likely due to the stream being shutdown: $exception"
          )
        ).onShutdown(
          // if a block has been queued while the system is being shutdown,
          // we may reach this point here, and we can safely just ignore the exception.
          logger.debug(
            s"error queueing block w/ height=${block.blockHeight}, but ignoring because queue has already been closed",
            exception,
          )
        )
    }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    Seq[AsyncOrSyncCloseable](
      SyncCloseable("queue.complete()", queue.complete()),
      AsyncCloseable(
        "queue.watchCompletion",
        queue.watchCompletion(),
        timeouts.shutdownProcessing,
      ),
    )
  }
}

object PekkoBlockSubscription {
  val BlockQueueBufferSize = 5000
}
