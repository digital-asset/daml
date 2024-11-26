// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import cats.data.{EitherT, Nested}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block
import com.digitalasset.canton.domain.block.BlockSequencerStateManager.HeadState
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  SequencerBlockStore,
}
import com.digitalasset.canton.domain.block.update.*
import com.digitalasset.canton.domain.block.update.BlockUpdateGenerator.BlockChunk
import com.digitalasset.canton.domain.sequencing.sequencer.{
  DeliverableSubmissionOutcome,
  InFlightAggregations,
  SequencerIntegration,
  SubmissionRequestOutcome,
}
import com.digitalasset.canton.domain.sequencing.traffic.store.TrafficConsumedStore
import com.digitalasset.canton.error.BaseAlarm
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, LoggerUtil}
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Thrown if the ephemeral state does not match what is expected in the persisted store.
  * This is not expected to be able to occur, but if it does likely means that the
  * ephemeral state is inconsistent with the persisted state.
  * The sequencer should be restarted and logs verified to ensure that the persisted state is correct.
  */
class SequencerUnexpectedStateChange(message: String = "Sequencer state has unexpectedly changed")
    extends RuntimeException(message)

/** State manager for operating a sequencer using Blockchain based infrastructure (such as fabric or ethereum) */
trait BlockSequencerStateManagerBase extends FlagCloseable {

  def getHeadState: HeadState

  /** Flow to turn [[com.digitalasset.canton.domain.block.BlockEvents]] of one block
    * into a series of [[update.OrderedBlockUpdate]]s
    * that are to be persisted subsequently using [[applyBlockUpdate]].
    */
  def processBlock(
      bug: BlockUpdateGenerator
  ): Flow[BlockEvents, Traced[OrderedBlockUpdate], NotUsed]

  /** Persists the [[update.BlockUpdate]]s and completes the waiting RPC calls
    * as necessary.
    */
  def applyBlockUpdate(
      dbSequencerIntegration: SequencerIntegration
  ): Flow[Traced[BlockUpdate], Traced[CantonTimestamp], NotUsed]

  /** Wait for the member's acknowledgement to have been processed */
  def waitForAcknowledgementToComplete(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]
}

class BlockSequencerStateManager(
    domainId: DomainId,
    val store: SequencerBlockStore,
    val trafficConsumedStore: TrafficConsumedStore,
    enableInvariantCheck: Boolean,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends BlockSequencerStateManagerBase
    with NamedLogging {

  import BlockSequencerStateManager.*

  private val memberAcknowledgementPromises =
    TrieMap[Member, NonEmpty[SortedMap[CantonTimestamp, Traced[Promise[Unit]]]]]()

  private val headState = new AtomicReference[HeadState]({
    import TraceContext.Implicits.Empty.*
    val headBlock =
      timeouts.unbounded.await(s"Reading the head of the $domainId sequencer state")(store.readHead)
    logger.debug(s"Initialized the block sequencer with head block ${headBlock.latestBlock}")
    HeadState.fullyProcessed(headBlock)
  })

  override def getHeadState: HeadState = headState.get()

  override def processBlock(
      bug: BlockUpdateGenerator
  ): Flow[BlockEvents, Traced[OrderedBlockUpdate], NotUsed] = {
    val head = getHeadState
    val bugState = {
      import TraceContext.Implicits.Empty.*
      bug.internalStateFor(head.blockEphemeralState)
    }
    Flow[BlockEvents]
      .via(checkBlockHeight(head.block.height))
      .via(chunkBlock(bug))
      .via(processChunk(bug)(bugState))
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
          implicit val traceContext: TraceContext = TraceContext.ofBatch(blockEvents.events)(logger)
          // Set the current block height to the new block's height instead of + 1 of the previous value
          // so that we support starting from an arbitrary block height

          logger.debug(
            s"Processing block $height with ${blockEvents.events.size} block events.${blockEvents.events
                .map(_.value)
                .collectFirst { case LedgerBlockEvent.Send(timestamp, _, _) =>
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
    Flow[Traced[BlockUpdate]].statefulMapAsync(getHeadState) { (priorHead, update) =>
      implicit val traceContext = update.traceContext
      val currentBlockNumber = priorHead.block.height + 1
      val fut = update.value match {
        case chunk: ChunkUpdate =>
          val chunkNumber = priorHead.chunk.chunkNumber + 1
          LoggerUtil.clueF(
            s"Adding block updates for chunk $chunkNumber for block $currentBlockNumber. " +
              s"Contains ${chunk.acknowledgements.size} acks, " +
              s"and ${chunk.inFlightAggregationUpdates.size} in-flight aggregation updates"
          )(handleChunkUpdate(priorHead, chunk, dbSequencerIntegration)(traceContext))
        case complete: CompleteBlockUpdate =>
          // TODO(#18401): Consider: wait for the DBS watermark to be updated to the blocks last timestamp
          //  in a supervisory manner, to detect things not functioning properly
          LoggerUtil.clueF(
            s"Storing completion of block $currentBlockNumber"
          )(handleComplete(priorHead, complete.block)(traceContext))
      }
      fut.map(newHead => newHead -> Traced(newHead.block.lastTs))
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
  ): Future[HeadState] = {
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
      case SubmissionRequestOutcome(_, _, outcome: DeliverableSubmissionOutcome) =>
        outcome.trafficReceiptO match {
          case Some(trafficReceipt) =>
            Some(
              trafficReceipt.toTrafficConsumed(outcome.submission.sender, outcome.sequencingTime)
            )
          case None => None
        }
      case _ => None
    }

    (for {
      _ <- EitherT.right[String](
        performUnlessClosingF("trafficConsumedStore.store")(
          trafficConsumedStore.store(trafficConsumedUpdates)
        )
      )
      _ <- dbSequencerIntegration.blockSequencerWrites(update.submissionsOutcomes.map(_.outcome))
      _ <- EitherT.right[String](
        dbSequencerIntegration.blockSequencerAcknowledge(update.acknowledgements)
      )

      _ <- EitherT.right[String](
        performUnlessClosingF("partialBlockUpdate")(
          store.partialBlockUpdate(inFlightAggregationUpdates = update.inFlightAggregationUpdates)
        )
      )
    } yield {
      val newHead = priorHead.copy(chunk = newState)
      updateHeadState(priorHead, newHead)
      update.acknowledgements.foreach { case (member, timestamp) =>
        resolveAcknowledgements(member, timestamp)
      }
      update.invalidAcknowledgements.foreach { case (member, timestamp, error) =>
        invalidAcknowledgement(member, timestamp, error)
      }
      newHead
    }).valueOr(e =>
      ErrorUtil.internalError(new RuntimeException(s"handleChunkUpdate failed with error: $e"))
    ).onShutdown {
      logger.info(s"handleChunkUpdate skipped due to shut down")
      priorHead
    }
  }

  private def handleComplete(priorHead: HeadState, newBlock: BlockInfo)(implicit
      blockTraceContext: TraceContext
  ): Future[HeadState] = {
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
    for {
      _ <- store.finalizeBlockUpdate(newBlock)
    } yield {
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

  /** Resolves all outstanding acknowledgements up to the given timestamp.
    * Unlike for resolutions of other requests, we resolve also all earlier acknowledgements,
    * because this mimics the effect of the acknowledgement: all earlier acknowledgements are irrelevant now.
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

  def apply(
      domainId: DomainId,
      store: SequencerBlockStore,
      trafficConsumedStore: TrafficConsumedStore,
      enableInvariantCheck: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): BlockSequencerStateManager =
    new BlockSequencerStateManager(
      domainId = domainId,
      store = store,
      trafficConsumedStore = trafficConsumedStore,
      enableInvariantCheck = enableInvariantCheck,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )

  /** Keeps track of the accumulated state changes by processing chunks of updates from a block
    *
    * @param chunkNumber The sequence number of the chunk
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
    * @param block Describes the state after the latest block that was fully processed.
    * @param chunk Describes the state after the last chunk of the block that is currently being processed.
    *              When the latest block is fully processed, but no chunks of the next block,
    *              then this is `ChunkState.initial`
    *              based on the last block's [[com.digitalasset.canton.domain.block.data.BlockEphemeralState]].
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
