// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  HasSynchronizeWithClosing,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.client.RichSequencerClientImpl
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success}

trait PostAggregationHandler {

  /** Future that completes when the handler is idle.
    */
  def handlerIsIdleF: Future[Unit]

  def signalHandler()(implicit traceContext: TraceContext): Unit
}

class PostAggregationHandlerImpl private[sequencing] (
    sequencerAggregator: SequencerAggregator,
    addToFlushAndLogError: String => Future[?] => Unit,
    eventInboxSize: PositiveInt,
    eventBatchProcessor: RichSequencerClientImpl.EventBatchProcessor,
    hasSynchronizeWithClosing: HasSynchronizeWithClosing,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends PostAggregationHandler
    with NamedLogging {

  // TODO(i27260): This link should not be needed, and we should ideally create the aggregator after the
  //  post-aggregation handler in `SequencerClient`, passing the latter as argument.
  sequencerAggregator.setPostAggregationHandler(this)

  /** Completed iff the handler is idle. */
  private val handlerIdle: AtomicReference[Promise[Unit]] = new AtomicReference(
    Promise.successful(())
  )
  private val handlerIdleLock: Object = new Object

  override def handlerIsIdleF: Future[Unit] = handlerIdle.get.future

  // TODO(i27260): This comment was taken straight from `SequencerClient.SubscriptionHandler`. It no longer
  //  makes sense, refers to elements that are not in this class, and should be cleaned-up/reworded.

  // Here is how shutdown works:
  //   1. we stop injecting new events even if the handler is idle using the synchronizeWithClosingSync,
  //   2. the synchronous processing will mark handlerIdle as not completed, and once started, will be added to the flush
  //      the synchronizeWithClosingSync will guard us from entering the close method (and starting to flush) before we've successfully
  //      registered with the flush future
  //   3. once the synchronous processing finishes, it will mark the `handlerIdle` as completed and complete the flush future
  //   4. before the synchronous processing terminates and before it marks the handler to be idle again,
  //      it will add the async processing to the flush future.
  //   Consequently, on shutdown, we first have to wait on the flush future.
  //     a. No synchronous event will be added to the flush future anymore by the signalHandler
  //        due to the synchronizeWithClosingSync. Therefore, we can be sure that once the flush future terminates
  //        during shutdown, that the synchronous processing has completed and nothing new has been added.
  //     b. However, the synchronous event processing will be adding async processing to the flush future in the
  //        meantime. This means that the flush future we are waiting on might be outdated.
  //        Therefore, we have to wait on the flush future again. We can then be sure that all asynchronous
  //        futures have been added in the meantime as the synchronous flush future finished.
  //     c. I (rv) think that waiting on the `handlerIdle` is a unnecessary for shutdown as it does the
  //        same as the flush future. We only need it to ensure we don't start the sequential processing in parallel.
  override def signalHandler()(implicit traceContext: TraceContext): Unit =
    hasSynchronizeWithClosing
      .synchronizeWithClosingSync(functionFullName) {
        logger.debug("Application handler has been signalled")
        val isIdle = blocking {
          handlerIdleLock.synchronized {
            val oldPromise = handlerIdle.getAndUpdate(p => if (p.isCompleted) Promise() else p)
            oldPromise.isCompleted
          }
        }
        if (isIdle) {
          val handlingF = handleReceivedEventsUntilEmpty()
          addToFlushAndLogError("invoking the application handler")(
            handlingF.failOnShutdownToAbortException("sequencer client: flush and log errors")
          )
        }
      }
      .discard

  private def handleReceivedEventsUntilEmpty(): FutureUnlessShutdown[Unit] = {
    val inboxSize = eventInboxSize.unwrap
    val javaEventList = new java.util.ArrayList[SequencedSerializedEvent](inboxSize)
    if (sequencerAggregator.eventQueue.drainTo(javaEventList, inboxSize) > 0) {
      import scala.jdk.CollectionConverters.*
      val handlerEvents = javaEventList.asScala.toSeq

      def stopHandler(): Unit = blocking {
        handlerIdleLock.synchronized(handlerIdle.get().success(()).discard)
      }

      eventBatchProcessor
        .process(handlerEvents)
        .value
        .transformWith {
          case Success(UnlessShutdown.Outcome(Right(()))) =>
            handleReceivedEventsUntilEmpty()
          case Success(UnlessShutdown.Outcome(Left(_))) | Failure(_) |
              Success(UnlessShutdown.AbortedDueToShutdown) =>
            // `eventBatchProcessor` has already propagated the error
            stopHandler()
            FutureUnlessShutdown.unit
        }
    } else {
      val stillBusy = blocking {
        handlerIdleLock.synchronized {
          val idlePromise = handlerIdle.get()
          if (sequencerAggregator.eventQueue.isEmpty) {
            // signalHandler must not be executed here, because that would lead to lost signals.
            idlePromise.success(())
          }
          // signalHandler must not be executed here, because that would lead to duplicate invocations.
          !idlePromise.isCompleted
        }
      }

      if (stillBusy) {
        handleReceivedEventsUntilEmpty()
      } else {
        FutureUnlessShutdown.unit
      }
    }
  }
}
