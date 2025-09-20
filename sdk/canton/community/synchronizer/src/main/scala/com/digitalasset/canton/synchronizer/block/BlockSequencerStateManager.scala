// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block

import cats.data.{EitherT, Nested}
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.BaseAlarm
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{
  CloseContext,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.InstrumentedGraph.BufferedFlow
import com.digitalasset.canton.sequencing.traffic.TrafficConsumed
import com.digitalasset.canton.synchronizer.block
import com.digitalasset.canton.synchronizer.block.AsyncWriter.AsyncAppendWorkHandle
import com.digitalasset.canton.synchronizer.block.BlockSequencerStateManager.HeadState
import com.digitalasset.canton.synchronizer.block.data.{
  BlockEphemeralState,
  BlockInfo,
  SequencerBlockStore,
}
import com.digitalasset.canton.synchronizer.block.update.*
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGenerator.BlockChunk
import com.digitalasset.canton.synchronizer.metrics.BlockMetrics
import com.digitalasset.canton.synchronizer.sequencer.{
  BlockSequencerStreamInstrumentationConfig,
  DeliverableSubmissionOutcome,
  InFlightAggregationUpdates,
  InFlightAggregations,
  SequencerIntegration,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{
  EitherTUtil,
  ErrorUtil,
  FutureUnlessShutdownUtil,
  LoggerUtil,
  MonadUtil,
}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Thrown if the ephemeral state does not match what is expected in the persisted store. This is
  * not expected to be able to occur, but if it does likely means that the ephemeral state is
  * inconsistent with the persisted state. The sequencer should be restarted and logs verified to
  * ensure that the persisted state is correct.
  */
class SequencerUnexpectedStateChange(message: String = "Sequencer state has unexpectedly changed")
    extends RuntimeException(message)

/** State manager for operating a sequencer using Blockchain based infrastructure (such as fabric or
  * ethereum)
  */
trait BlockSequencerStateManagerBase extends FlagCloseable {

  def getHeadState: HeadState

  /** Flow to turn [[BlockEvents]] of one block into a series of [[update.OrderedBlockUpdate]]s that
    * are to be persisted subsequently using [[applyBlockUpdate]].
    */
  def processBlock(
      bug: BlockUpdateGenerator
  ): Flow[BlockEvents, Traced[OrderedBlockUpdate], NotUsed]

  /** Persists the [[update.BlockUpdate]]s and completes the waiting RPC calls as necessary.
    */
  def applyBlockUpdate(
      dbSequencerIntegration: SequencerIntegration
  ): Flow[Traced[BlockUpdate], Traced[CantonTimestamp], NotUsed]

  /** Wait for the member's acknowledgement to have been processed */
  def waitForAcknowledgementToComplete(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]
}

/** Async block sequencer writer control parameters
  *
  * @param enabled
  *   if true then the async writer is enabled
  * @param trafficBatchSize
  *   the maximum number of traffic events to batch in a single write
  * @param aggregationBatchSize
  *   the maximum number of inflight aggregations to batch in a single write
  * @param blockInfoBatchSize
  *   the maximum number of block info updates to batch in a single write
  */
final case class AsyncWriterParameters(
    enabled: Boolean = true,
    trafficBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
    aggregationBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
    blockInfoBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
)

/** async sequential writer for one type of queries
  *
  * @param addToQueue
  *   a function that adds an element to the queue
  * @param writeQueue
  *   a function that will write the queue
  */
private[block] abstract class AsyncWriter[Q <: Iterable[?]](
    addToQueue: (Q, Q) => Q,
    writeQueue: Q => FutureUnlessShutdown[Unit],
    empty: => Q,
    name: String,
    futureSupervisor: FutureSupervisor,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, closeContext: CloseContext)
    extends NamedLogging
    with NoTracing {

  import AsyncWriter.*

  private val writeCompletedPromiseDesc = name + "-write-completed"
  private val queueScheduledPromiseDesc = name + "-queue-scheduled"

  private val queuedDataState =
    new AtomicReference[QueuedData[Q]](QueuedData.Idle)

  private def mkPromise[A](description: String) =
    PromiseUnlessShutdown
      .abortOnShutdown[A](description, closeContext.context, futureSupervisor)

  private def newQueueData(): QueuedData[Q] =
    QueuedData.Running(
      mkPromise(writeCompletedPromiseDesc),
      mkPromise(queueScheduledPromiseDesc),
      empty,
    )

  /** Append to the queue and schedule if necessary
    *
    * @param items
    *   new items to be added to the queue
    *
    * @return
    *   an async append result
    */
  def appendAndSchedule(items: Q): AsyncAppendWorkHandle =
    if (items.isEmpty) {
      // if we are called to enqueue without any item, just return a completed work handle
      AsyncAppendComplete
    } else {

      def go(
          newItems: Option[Q] // None means we completed a write
      ): AsyncAppendWorkHandle = {
        val currentQueueData =
          queuedDataState.getAndUpdate {

            case QueuedData.Running(promiseCompleted, promiseSubmitted, pending) =>
              newItems.fold {
                if (pending.isEmpty)
                  // we completed the write and no pending => stopped
                  QueuedData.Idle
                else {
                  // we completed the write but there are pending writes => keep running and reset the queue
                  //  as we'll dispatch the pending items
                  newQueueData()
                }
              } { newItems =>
                // we are already running and are just adding more items
                QueuedData.Running(
                  promiseCompleted,
                  promiseSubmitted,
                  addToQueue(newItems, pending),
                )
              }
            // we are not running, and we are adding items => start as we'll dispatch the new items
            case QueuedData.Idle =>
              assert(newItems.isDefined)
              newQueueData()
          }

        currentQueueData match {

          case QueuedData.Running(queueCompleted, queueSubmitted, pending) =>
            newItems.fold {
              // nothing left to do, so we finish here
              if (pending.isEmpty) {
                AsyncAppendComplete
              } else {
                // 1b something left to do, so we pick up the queue and notify anyone who is waiting
                // on the queue being picked up
                queueSubmitted.outcome(()).discard
                AsyncAppendWorkHandle(
                  dispatchQueue(pending, Some(queueCompleted)),
                  FutureUnlessShutdown.unit,
                  0,
                )
              }
            } { newItems =>
              // we appended to an already running queue, therefore just return the current futures
              AsyncAppendWorkHandle(
                queueCompleted.futureUS,
                queueSubmitted.futureUS,
                pending.size + newItems.size,
              )
            }
          case QueuedData.Idle =>
            // we are not running and we are adding more items => start
            AsyncAppendWorkHandle(
              dispatchQueue(newItems.getOrElse(empty), None),
              FutureUnlessShutdown.unit,
              0,
            )
        }
      }

      // Returns a future of the persisted queue
      def dispatchQueue(
          queue: Q,
          completePromise: Option[PromiseUnlessShutdown[Unit]],
      ): FutureUnlessShutdown[Unit] =
        writeQueue(queue)
          .thereafter {
            case Success(Outcome(_)) =>
              // we completed the write, so we can complete the promise
              completePromise.foreach(_.outcome(()))
              // respawn if there are pending items
              FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
                go(newItems = None).queuePersisted,
                "background-writer-respawn-" + name,
              )(this.errorLoggingContext(TraceContext.empty))
            case Success(UnlessShutdown.AbortedDueToShutdown) =>
              completePromise.foreach(_.shutdown())
            case Failure(exception) =>
              recordWriteError(name, exception)
              completePromise.foreach(_.failure(exception).discard)
          }

      go(Some(items))
    }

  protected def recordWriteError(name: String, exception: Throwable): Unit
}

object AsyncWriter {

  private[block] final case class AsyncAppendWorkHandle(
      queuePersisted: FutureUnlessShutdown[Unit],
      queueSubmitted: FutureUnlessShutdown[Unit],
      queueSize: Int,
  ) {
    def backpressureFU(maxQueueSize: Int): FutureUnlessShutdown[Unit] =
      // if the number of items is larger than our queue, ensure that we wait until
      // the queue has been picked up. This provides some form of backpressure as it will
      // halt adding more items to the queue until the next write has started
      if (queueSize >= maxQueueSize) {
        queueSubmitted
      } else {
        FutureUnlessShutdown.unit
      }
  }

  private[block] val AsyncAppendComplete =
    AsyncAppendWorkHandle(FutureUnlessShutdown.unit, FutureUnlessShutdown.unit, 0)

  private sealed trait QueuedData[+T] extends Product with Serializable {
    def queueSize: Int
  }
  private object QueuedData {

    /** Case class to store pending data in the queue
      *
      * @param queuePersisted
      *   a promise which will be completed once all elements of the queue have been persisted; this
      *   is used to synchronize the "blockInfo" writes
      * @param queueSubmitted
      *   a promise which will be completed once the queue has been picked up for writing; this is
      *   used to provide backpressure of the akka pipeline if the write queue becomes too large
      * @param queue
      *   the actual queue
      */
    final case class Running[+T <: Iterable[?]](
        queuePersisted: PromiseUnlessShutdown[Unit],
        queueSubmitted: PromiseUnlessShutdown[Unit],
        queue: T,
    ) extends QueuedData[T] {
      override def queueSize: Int = queue.size
    }
    final case object Idle extends QueuedData[Nothing] {
      override def queueSize: Int = 0
    }
  }

}

/** writes updates asynchronusly into the database
  *
  * to decouple the sequencer processing pipeline from the database writes, we schedule the writes
  * all in background while the main pipeline is still running.
  *
  * the block-info serves as the watermark and is only written once all previous writes have been
  * persisted.
  *
  * the writes are still sequential, but sequential per "type" of write, batching writes of
  * different blocks together if necessary.
  *
  * a further optimisation potential is to actually write everything in parallel, but this would
  * require establishing the correct dependencies between the writes and ensure that no read query
  * reads dirty state.
  *
  * but instead of doing this we should take a step back and look at the database storage schema
  * that we currently have and decide whether it actually makes sense.
  */
private class BlockSequencerStateAsyncWriter(
    store: SequencerBlockStore,
    trafficConsumedStore: TrafficConsumedStore,
    futureSupervisor: FutureSupervisor,
    parameters: AsyncWriterParameters,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, closeContext: CloseContext)
    extends NamedLogging {

  private def mkWriter[Q <: Iterable[?]](
      addToQueue: (Q, Q) => Q,
      writeQueue: Q => FutureUnlessShutdown[Unit],
      empty: => Q,
      name: String,
  ) = new AsyncWriter[Q](
    addToQueue,
    writeQueue,
    empty,
    name,
    futureSupervisor,
    loggerFactory,
  ) {
    override protected def recordWriteError(name: String, exception: Throwable): Unit = {
      noTracingLogger.error(
        "Background write failed - no further writes to avoid inconsistent store",
        exception,
      )
      observedError.set(
        Some(
          new Exception(
            s"Write $name failed - no further writes to avoid inconsistent store"
          ).initCause(exception)
        )
      )
    }
  }
  private def mkPromise[A](description: String) =
    PromiseUnlessShutdown.abortOnShutdown[A](description, closeContext.context, futureSupervisor)(
      errorLoggingContext(TraceContext.empty)
    )

  private val observedError = new AtomicReference[Option[Throwable]](None)
  private val pendingWrites =
    new AtomicReference[FutureUnlessShutdown[Unit]](FutureUnlessShutdown.unit)

  private val trafficWriter = mkWriter[Vector[TrafficConsumed]](
    (newItem, queue) => queue ++ newItem,
    trafficConsumedStore.store(_)(TraceContext.empty),
    empty = Vector.empty[TrafficConsumed],
    "traffic-consumed-writer",
  )
  private val aggregationWriter = mkWriter[InFlightAggregationUpdates](
    (newItem, queue) =>
      newItem.foldLeft(queue) { case (agg, (k, v)) =>
        agg.updatedWith(k) {
          case Some(value) => Some(value.tryMerge(v)(this.errorLoggingContext(TraceContext.empty)))
          case None => Some(v)
        }
      },
    store.storeInflightAggregations(_)(TraceContext.empty),
    empty = Map.empty,
    "in-flight-aggregation",
  )
  private val blockInfoWriter = {
    def writeQueue(
        queued: Seq[(FutureUnlessShutdown[Unit], BlockInfo)]
    ): FutureUnlessShutdown[Unit] =
      // we only write once the pending dependent writes have completed
      MonadUtil
        .sequentialTraverse_(queued) { case (write, _) => write }
        .flatMap(_ => store.finalizeBlockUpdates(queued.map(_._2))(TraceContext.empty))
    mkWriter[Vector[(FutureUnlessShutdown[Unit], BlockInfo)]](
      (newItem, queue) => queue ++ newItem,
      writeQueue,
      Vector.empty,
      "block-info",
    )
  }

  private def transformSync(
      fut: FutureUnlessShutdown[Unit],
      flatMapF: FutureUnlessShutdown[Unit],
  ): FutureUnlessShutdown[Unit] =
    // transform propagating errors synchronously
    fut.transformWith { previous =>
      flatMapF.transform {
        case Success(_) => previous
        case e @ Failure(_) => e
      }
    }

  private def addPendingWrite(
      writeAndQueue: AsyncAppendWorkHandle,
      maxQueueSize: PositiveInt,
  ): FutureUnlessShutdown[Unit] = {
    val res = writeAndQueue
    // update the pending writes to include this write
    // use a promise as otherwise we might schedule the flatmap multiple times
    val updated = mkPromise[Unit]("pending-writes-update")
    val fut = pendingWrites.getAndSet(updated.futureUS)
    // chain the previous writes with this write. If there is an error, it will be
    // propagated into the out most future which will be monitored via FutureUtil
    transformSync(fut, res.queuePersisted).thereafter(updated.complete).discard
    res.backpressureFU(maxQueueSize.value)
  }

  def append(
      trafficConsumedUpdates: Seq[TrafficConsumed],
      inFlightAggregationUpdates: InFlightAggregationUpdates,
      acknowledgementsET: EitherT[FutureUnlessShutdown, String, Unit],
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    observedError.get() match {
      // forward any background error
      case Some(err) =>
        EitherT[FutureUnlessShutdown, String, Unit](FutureUnlessShutdown.failed(err))
      case None =>
        // we enqueue or start the writing in the background. the returning future will be used to
        // sync the final writing of the block height.
        // if we have more items queued than the limit, we'll wait for the current queue to be picked up
        val backpressureF1 = addPendingWrite(
          trafficWriter.appendAndSchedule(trafficConsumedUpdates.toVector),
          parameters.trafficBatchSize,
        )
        val backpressureF2 = addPendingWrite(
          aggregationWriter.appendAndSchedule(inFlightAggregationUpdates),
          parameters.aggregationBatchSize,
        )
        // replace promise (don't flatmap in atomic reference to avoid duplicate scheduling)
        val promise = mkPromise[Unit]("append-acknowledgements")
        val fut = pendingWrites.getAndSet(promise.futureUS)
        // chain the previous writes with this write. If there is an error, it will be
        // propagated to the outer most future which will be monitored via FutureUtil
        transformSync(
          fut,
          EitherTUtil.toFutureUnlessShutdown(
            acknowledgementsET.leftMap(str => new Exception(str))
          ),
        ).thereafter(promise.complete).discard
        EitherT.right(transformSync(backpressureF1, backpressureF2))
    }

  def finalizeBlockUpate(newBlock: BlockInfo): FutureUnlessShutdown[Unit] =
    observedError.get() match {
      // forward any background error
      case Some(err) => FutureUnlessShutdown.failed(err)
      case None =>
        // this is safe as we will be called sequentially
        val writesF = pendingWrites.getAndSet(FutureUnlessShutdown.unit)
        val result = blockInfoWriter.appendAndSchedule(Vector((writesF, newBlock)))
        FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
          result.queuePersisted,
          "finalize-block-update failed",
        )(this.errorLoggingContext((TraceContext.empty)))
        result.backpressureFU(parameters.blockInfoBatchSize.value)
    }

}

class BlockSequencerStateManager(
    val store: SequencerBlockStore,
    val trafficConsumedStore: TrafficConsumedStore,
    asyncWriterParameters: AsyncWriterParameters,
    enableInvariantCheck: Boolean,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
    headState: AtomicReference[HeadState],
    streamInstrumentationConfig: BlockSequencerStreamInstrumentationConfig,
    blockMetrics: BlockMetrics,
)(implicit executionContext: ExecutionContext)
    extends BlockSequencerStateManagerBase
    with NamedLogging
    with HasCloseContext {

  import BlockSequencerStateManager.*

  private val asyncWriter = Option.when(asyncWriterParameters.enabled)(
    new BlockSequencerStateAsyncWriter(
      store = store,
      trafficConsumedStore = trafficConsumedStore,
      futureSupervisor,
      asyncWriterParameters,
      loggerFactory,
    )
  )

  private val memberAcknowledgementPromises =
    TrieMap[Member, NonEmpty[SortedMap[CantonTimestamp, Traced[Promise[Unit]]]]]()

  override def getHeadState: HeadState = headState.get()

  override def processBlock(
      bug: BlockUpdateGenerator
  ): Flow[BlockEvents, Traced[OrderedBlockUpdate], NotUsed] = {
    val head = getHeadState
    val bugState = {
      import TraceContext.Implicits.Empty.*
      bug.internalStateFor(head.blockEphemeralState)
    }

    def finalFlow[In, Out, Mat](
        original: Flow[In, Out, Mat],
        flowName: String,
    ): Flow[In, Out, Mat] =
      if (streamInstrumentationConfig.isEnabled)
        original.buffered(
          blockMetrics.stramBufferSize,
          streamInstrumentationConfig.bufferSize.value,
        )(MetricsContext("element" -> flowName))
      else original

    Flow[BlockEvents]
      .via(
        finalFlow(checkBlockHeight(head.block.height), "check_block_height")
      )
      .via(
        finalFlow(chunkBlock(bug), "chunk_block")
      )
      .via(
        finalFlow(processChunk(bug)(bugState), "process_chunk")
      )
  }

  private def checkBlockHeight(
      initialHeight: Long
  ): Flow[BlockEvents, Traced[BlockEvents], NotUsed] =
    Flow[BlockEvents].statefulMapConcat { () =>
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var currentBlockHeight = initialHeight
      blockEvents => {

        val height = blockEvents.height

        // TODO(M98 Tech-Debt Collection): consider validating that blocks with the same block height have the same contents
        // Skipping blocks we have processed before. Can occur when the read-path flowable is re-started but not all blocks
        // in the pipeline of the BlockSequencerStateManager have already been processed.
        if (height <= currentBlockHeight) {
          noTracingLogger.debug(
            s"Skipping update with height $height since it was already processed. "
          )
          Seq.empty
        } else if (
          currentBlockHeight > block.UninitializedBlockHeight && height > currentBlockHeight + 1
        ) {
          val msg =
            s"Received block of height $height while the last processed block only had height $currentBlockHeight. " +
              s"Expected to receive one block higher only."
          noTracingLogger.error(msg)
          throw new SequencerUnexpectedStateChange(msg)
        } else {
          implicit val traceContext: TraceContext =
            TraceContext.ofBatch("check_block_height")(blockEvents.events)(logger)
          // Set the current block height to the new block's height instead of + 1 of the previous value
          // so that we support starting from an arbitrary block height
          logger.debug(
            s"Processing block $height with ${blockEvents.events.size} block events.${blockEvents.events
                .map(_.value)
                .collectFirst { case LedgerBlockEvent.Send(timestamp, _, _, _) =>
                  s" First timestamp in block: $timestamp"
                }
                .getOrElse("")}"
          )
          currentBlockHeight = height
          Seq(Traced(blockEvents))
        }
      }
    }

  private def chunkBlock(
      bug: BlockUpdateGenerator
  ): Flow[Traced[BlockEvents], Traced[BlockChunk], NotUsed] =
    Flow[Traced[BlockEvents]].mapConcat(_.withTraceContext { implicit traceContext => blockEvents =>
      bug.chunkBlock(blockEvents).map(Traced(_))
    })

  private def processChunk(bug: BlockUpdateGenerator)(
      initialState: bug.InternalState
  ): Flow[Traced[BlockChunk], Traced[OrderedBlockUpdate], NotUsed] = {
    implicit val traceContext: TraceContext = TraceContext.empty
    Flow[Traced[BlockChunk]].statefulMapAsyncUSAndDrain(initialState) { (state, tracedChunk) =>
      implicit val traceContext: TraceContext = tracedChunk.traceContext
      tracedChunk.traverse(blockChunk => Nested(bug.processBlockChunk(state, blockChunk))).value
    }
  }

  override def applyBlockUpdate(
      dbSequencerIntegration: SequencerIntegration
  ): Flow[Traced[BlockUpdate], Traced[CantonTimestamp], NotUsed] = {
    implicit val traceContext = TraceContext.empty
    Flow[Traced[BlockUpdate]].statefulMapAsyncUSAndDrain(getHeadState) { (priorHead, update) =>
      implicit val traceContext = update.traceContext
      val currentBlockNumber = priorHead.block.height + 1
      val fut = update.value match {
        case chunk: ChunkUpdate =>
          val chunkNumber = priorHead.chunk.chunkNumber + 1
          LoggerUtil.clueUSF(
            s"Adding block updates for chunk $chunkNumber for block $currentBlockNumber. " +
              s"Contains ${chunk.acknowledgements.size} acks, " +
              s"and ${chunk.inFlightAggregationUpdates.size} in-flight aggregation updates"
          )(handleChunkUpdate(priorHead, chunk, dbSequencerIntegration)(traceContext))
        case complete: CompleteBlockUpdate =>
          // TODO(#18401): Consider: wait for the DBS watermark to be updated to the blocks last timestamp
          //  in a supervisory manner, to detect things not functioning properly
          LoggerUtil.clueUSF(
            s"Storing completion of block $currentBlockNumber"
          )(handleComplete(priorHead, complete.block)(traceContext))
      }
      fut
        .map(newHead => newHead -> Traced(newHead.block.lastTs))
    }
  }

  override def waitForAcknowledgementToComplete(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    memberAcknowledgementPromises
      .updateWith(member) {
        case None => Some(NonEmpty(SortedMap, timestamp -> Traced(Promise[Unit]())))
        case Some(promises) =>
          Some(
            if (promises.contains(timestamp)) promises
            else promises.updated(timestamp, Traced(Promise[Unit]()))
          )
      }
      .getOrElse(
        ErrorUtil.internalError(
          new NoSuchElementException(
            "The updateWith function returned None despite the update rule always returning a Some"
          )
        )
      )(timestamp)
      .value
      .future

  private def handleChunkUpdate(
      priorHead: HeadState,
      update: ChunkUpdate,
      dbSequencerIntegration: SequencerIntegration,
  )(implicit
      batchTraceContext: TraceContext
  ): FutureUnlessShutdown[HeadState] = {
    val priorState = priorHead.chunk
    val chunkNumber = priorState.chunkNumber + 1
    val currentBlockNumber = priorHead.block.height + 1
    assert(
      update.lastSequencerEventTimestamp.forall(last =>
        priorState.latestSequencerEventTimestamp.forall(_ < last)
      ),
      s"The last sequencer's event timestamp ${update.lastSequencerEventTimestamp} in chunk $chunkNumber of block $currentBlockNumber  must be later than the previous chunk's or block's latest sequencer event timestamp at ${priorState.latestSequencerEventTimestamp}",
    )

    val lastTs = priorState.lastTs

    val newState = ChunkState(
      chunkNumber,
      update.inFlightAggregations,
      lastTs,
      update.lastSequencerEventTimestamp.orElse(priorState.latestSequencerEventTimestamp),
    )

    val trafficConsumedUpdates = update.submissionsOutcomes.flatMap {
      case outcome: DeliverableSubmissionOutcome =>
        outcome.trafficReceiptO match {
          case Some(trafficReceipt) =>
            Some(
              trafficReceipt.toTrafficConsumed(outcome.submission.sender, outcome.sequencingTime)
            )
          case None => None
        }
      case _ => None
    }

    def writeSequential() = {
      val trafficConsumedFUS = EitherT.right[String](
        synchronizeWithClosing("trafficConsumedStore.store")(
          trafficConsumedStore.store(trafficConsumedUpdates)
        )
      )
      val blockSequencerWritesFUS =
        dbSequencerIntegration.blockSequencerWrites(update.submissionsOutcomes)
      val blockSequencerAcknowledgementsFUS = EitherT.right[String](
        dbSequencerIntegration.blockSequencerAcknowledge(update.acknowledgements)
      )
      val inFlightAggregationUpdatesFUS = EitherT.right[String](
        synchronizeWithClosing("storeInflightAggregations")(
          store.storeInflightAggregations(inFlightAggregationUpdates =
            update.inFlightAggregationUpdates
          )
        )
      )
      (for {
        _ <- trafficConsumedFUS
        _ <- blockSequencerWritesFUS
        _ <- blockSequencerAcknowledgementsFUS
        _ <- inFlightAggregationUpdatesFUS
      } yield ())
    }

    def writeAsync(asyncWriter: BlockSequencerStateAsyncWriter) = {
      val acknowledgementsET = EitherT.right[String](
        dbSequencerIntegration.blockSequencerAcknowledge(update.acknowledgements)
      )
      // the return value is just there to abort errors
      val asyncErrorET = asyncWriter.append(
        trafficConsumedUpdates,
        update.inFlightAggregationUpdates,
        acknowledgementsET,
      )
      (for {
        // note: these writes are non-blocking. they will just be put into a queue but backpressure if the queue is full
        _ <- dbSequencerIntegration.blockSequencerWrites(update.submissionsOutcomes)
        _ <- asyncErrorET
      } yield ())
    }

    val writeET = asyncWriter match {
      case None =>
        writeSequential()
      case Some(asyncWriter) =>
        writeAsync(asyncWriter)
    }

    writeET
      .map { _ =>
        val newHead = priorHead.copy(chunk = newState)
        updateHeadState(priorHead, newHead)
        update.acknowledgements.foreach { case (member, timestamp) =>
          resolveAcknowledgements(member, timestamp)
        }
        update.invalidAcknowledgements.foreach { case (member, timestamp, error) =>
          invalidAcknowledgement(member, timestamp, error)
        }
        newHead
      }
      .valueOr(e =>
        ErrorUtil.internalError(new RuntimeException(s"handleChunkUpdate failed with error: $e"))
      )
  }

  private def handleComplete(priorHead: HeadState, newBlock: BlockInfo)(implicit
      blockTraceContext: TraceContext
  ): FutureUnlessShutdown[HeadState] = {
    val chunkState = priorHead.chunk
    assert(
      chunkState.lastTs <= newBlock.lastTs,
      s"The block's last timestamp must be at least the last timestamp of the last chunk",
    )
    assert(
      chunkState.latestSequencerEventTimestamp <= newBlock.latestSequencerEventTimestamp,
      s"The block's latest topology client timestamp must be at least the last chunk's latest topology client timestamp",
    )

    val newState = BlockEphemeralState(
      newBlock,
      chunkState.inFlightAggregations,
    )
    checkInvariantIfEnabled(newState)
    val newHead = HeadState.fullyProcessed(newState)

    (asyncWriter match {
      case Some(asyncWriter) =>
        // write is async. future only forwarded to inject future failed in case we are unable to write
        asyncWriter.finalizeBlockUpate(newBlock)
      case None =>
        store.finalizeBlockUpdates(Seq(newBlock))
    }).map { _ =>
      updateHeadState(priorHead, newHead)
      newHead
    }

  }

  private def updateHeadState(prior: HeadState, next: HeadState)(implicit
      traceContext: TraceContext
  ): Unit =
    if (!headState.compareAndSet(prior, next)) {
      // The write flow should not call this method concurrently so this situation should never happen.
      // If it does, this means that the ephemeral state has been updated since this update was generated,
      // and that the persisted state is now likely inconsistent.
      // throw exception to shutdown the sequencer write flow as we can not continue.
      ErrorUtil.internalError(new SequencerUnexpectedStateChange)
    }

  /** Resolves all outstanding acknowledgements up to the given timestamp. Unlike for resolutions of
    * other requests, we resolve also all earlier acknowledgements, because this mimics the effect
    * of the acknowledgement: all earlier acknowledgements are irrelevant now.
    */
  private def resolveAcknowledgements(member: Member, upToInclusive: CantonTimestamp)(implicit
      tc: TraceContext
  ): Unit = {
    // Use a `var` here to obtain the previous value associated with the `member`,
    // as `updateWith` returns the new value. We could implement our own version of `updateWith` instead,
    // but we'd rely on internal Scala collections API for this.
    //
    // Don't complete the promises inside the `updateWith` function
    // as this is a side effect and the function may be evaluated several times.
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var previousPromises: Option[SortedMap[CantonTimestamp, Traced[Promise[Unit]]]] = None
    logger.debug(s"Resolving an acknowledgment for member $member")
    memberAcknowledgementPromises
      .updateWith(member) { previous =>
        previousPromises = previous
        previous match {
          case None => None
          case Some(promises) =>
            val remaining = promises.dropWhile { case (timestamp, _) =>
              timestamp <= upToInclusive
            }
            NonEmpty.from(remaining)
        }
      }
      .discard
    previousPromises
      .getOrElse(SortedMap.empty[CantonTimestamp, Traced[Promise[Unit]]])
      .takeWhile { case (timestamp, _) => timestamp <= upToInclusive }
      .foreach { case (_, tracedPromise) =>
        tracedPromise.value.success(())
      }
  }

  /** Complete the acknowledgement promise for `member` and `ackTimestamp` with an error
    */
  private def invalidAcknowledgement(
      member: Member,
      ackTimestamp: CantonTimestamp,
      error: BaseAlarm,
  ): Unit = {
    // Use a `var` here to obtain the previous value associated with the `member`,
    // as `updateWith` returns the new value. We could implement our own version of `updateWith` instead,
    // but we'd rely on internal Scala collections API for this.
    //
    // Don't complete the promises inside the `updateWith` function
    // as this is a side effect and the function may be evaluated several times.
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var previousPromises: Option[SortedMap[CantonTimestamp, Traced[Promise[Unit]]]] = None
    memberAcknowledgementPromises
      .updateWith(member) { previous =>
        previousPromises = previous
        previous match {
          case None => None
          case Some(promises) =>
            if (promises.contains(ackTimestamp)) {
              NonEmpty.from(promises.removed(ackTimestamp))
            } else Some(promises)
        }
      }
      .discard
    previousPromises
      .getOrElse(SortedMap.empty[CantonTimestamp, Traced[Promise[Unit]]])
      .get(ackTimestamp)
      .foreach(_.withTraceContext { implicit traceContext => promise =>
        promise.failure(error.asGrpcError)
      })
  }

  private def checkInvariantIfEnabled(
      blockState: BlockEphemeralState
  )(implicit traceContext: TraceContext): Unit =
    if (enableInvariantCheck) blockState.checkInvariant()

}

object BlockSequencerStateManager {

  def create(
      synchronizerId: PhysicalSynchronizerId,
      store: SequencerBlockStore,
      trafficConsumedStore: TrafficConsumedStore,
      asyncWriterParameters: AsyncWriterParameters,
      enableInvariantCheck: Boolean,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
      streamInstrumentationConfig: BlockSequencerStreamInstrumentationConfig,
      blockMetrics: BlockMetrics,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): UnlessShutdown[BlockSequencerStateManager] = {
    val logger = loggerFactory.getTracedLogger(getClass)
    implicit val errorLoggingContext: ErrorLoggingContext =
      ErrorLoggingContext.fromTracedLogger(logger)
    timeouts.unbounded
      .awaitUS(s"Reading the head of the $synchronizerId sequencer state")(store.readHead)
      .map { headBlockO =>
        val headBlock = headBlockO.getOrElse(BlockEphemeralState.empty)
        new AtomicReference[HeadState]({
          logger.debug(
            s"Initialized the block sequencer with head block ${headBlock.latestBlock}"
          )
          HeadState.fullyProcessed(headBlock)
        })
      }
      .map { headState =>
        new BlockSequencerStateManager(
          store = store,
          trafficConsumedStore = trafficConsumedStore,
          asyncWriterParameters = asyncWriterParameters,
          enableInvariantCheck = enableInvariantCheck,
          timeouts = timeouts,
          futureSupervisor = futureSupervisor,
          loggerFactory = loggerFactory,
          headState = headState,
          streamInstrumentationConfig = streamInstrumentationConfig,
          blockMetrics = blockMetrics,
        )
      }
  }

  /** Keeps track of the accumulated state changes by processing chunks of updates from a block
    *
    * @param chunkNumber
    *   The sequence number of the chunk
    */
  final case class ChunkState(
      chunkNumber: Long,
      inFlightAggregations: InFlightAggregations,
      lastTs: CantonTimestamp,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )

  object ChunkState {
    val initialChunkCounter = 0L

    def initial(block: BlockEphemeralState): ChunkState =
      ChunkState(
        initialChunkCounter,
        block.inFlightAggregations,
        block.latestBlock.lastTs,
        block.latestBlock.latestSequencerEventTimestamp,
      )
  }

  /** The head state is updated after each chunk.
    *
    * @param block
    *   Describes the state after the latest block that was fully processed.
    * @param chunk
    *   Describes the state after the last chunk of the block that is currently being processed.
    *   When the latest block is fully processed, but no chunks of the next block, then this is
    *   `ChunkState.initial` based on the last block's
    *   [[com.digitalasset.canton.synchronizer.block.data.BlockEphemeralState]].
    */
  final case class HeadState(
      block: BlockInfo,
      chunk: ChunkState,
  ) {
    def blockEphemeralState(implicit
        loggingContext: ErrorLoggingContext
    ): BlockEphemeralState = {
      ErrorUtil.requireState(
        chunk.chunkNumber == ChunkState.initialChunkCounter,
        s"Cannot construct a BlockEphemeralState if there are partial block updates from ${chunk.chunkNumber} chunks.",
      )
      BlockEphemeralState(block, chunk.inFlightAggregations)
    }
  }

  object HeadState {
    def fullyProcessed(block: BlockEphemeralState): HeadState =
      HeadState(block.latestBlock, ChunkState.initial(block))
  }
}
