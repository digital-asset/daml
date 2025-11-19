// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{SyncCryptoApi, SynchronizerCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, LogicalUpgradeTime}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  GroupRecipient,
  SequencerDeliverError,
  SequencersOfSynchronizer,
}
import com.digitalasset.canton.synchronizer.block.BlockEvents.TickTopology
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.*
import com.digitalasset.canton.synchronizer.block.data.{BlockEphemeralState, BlockInfo}
import com.digitalasset.canton.synchronizer.block.{BlockEvents, LedgerBlockEvent, RawLedgerBlock}
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError.{
  InvalidLedgerEvent,
  SequencedBeforeOrAtLowerBound,
}
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.synchronizer.sequencer.{InFlightAggregations, SubmissionOutcome}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.digitalasset.canton.util.collection.IterableUtil
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/** Exposes functions that take the deserialized contents of a block from a blockchain integration
  * and compute the new [[BlockUpdate]]s.
  *
  * These functions correspond to the following steps in the block processing stream pipeline:
  *   1. Extracting block events from a raw ledger block ([[extractBlockEvents]]).
  *   1. Chunking such block events into either event chunks terminated by a sequencer-addessed
  *      event or a block completion ([[chunkBlock]]).
  *   1. Validating and enriching chunks to yield block updates ([[processBlockChunk]]).
  *
  * In particular, these functions are responsible for the final timestamp assignment of a given
  * submission request. The timestamp assignment works as follows:
  *   1. an initial timestamp is assigned to the submission request by the sequencer that writes it
  *      to the ledger
  *   1. each sequencer that reads the block potentially adapts the previously assigned timestamp
  *      deterministically via `ensureStrictlyIncreasingTimestamp`
  *   1. this timestamp is used to compute the [[BlockUpdate]]s
  *
  * Reasoning:
  *   - Step 1 is done so that every sequencer sees the same timestamp for a given event.
  *   - Step 2 is needed because different sequencers may assign the same timestamps to different
  *     events or may not assign strictly increasing timestamps due to clock skews.
  *
  * Invariants: For step 2, we assume that every sequencer observes the same stream of events from
  * the underlying ledger (and especially that events are always read in the same order).
  */
trait BlockUpdateGenerator {
  import BlockUpdateGenerator.*

  type InternalState

  def internalStateFor(state: BlockEphemeralState): InternalState

  def extractBlockEvents(tracedBlock: Traced[RawLedgerBlock]): Traced[BlockEvents]

  def chunkBlock(block: BlockEvents)(implicit
      traceContext: TraceContext
  ): immutable.Iterable[BlockChunk]

  def processBlockChunk(state: InternalState, chunk: BlockChunk)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(InternalState, OrderedBlockUpdate)]
}

object BlockUpdateGenerator {

  sealed trait BlockChunk extends Product with Serializable
  final case class NextChunk(
      blockHeight: Long,
      chunkIndex: Int,
      events: NonEmpty[Seq[Traced[LedgerBlockEvent]]],
  ) extends BlockChunk
  final case class TopologyTickChunk(
      blockHeight: Long,
      tickAtLeastAt: CantonTimestamp,
      groupRecipient: Either[AllMembersOfSynchronizer.type, SequencersOfSynchronizer.type],
  ) extends BlockChunk
  final case class MaybeTopologyTickChunk(blockHeight: Long) extends BlockChunk
  final case class EndOfBlock(blockHeight: Long) extends BlockChunk
}

class BlockUpdateGeneratorImpl(
    protocolVersion: ProtocolVersion,
    synchronizerSyncCryptoApi: SynchronizerCryptoClient,
    sequencerId: SequencerId,
    rateLimitManager: SequencerRateLimitManager,
    orderingTimeFixMode: OrderingTimeFixMode,
    sequencingTimeLowerBoundExclusive: Option[CantonTimestamp],
    useTimeProofsToObserveEffectiveTime: Boolean,
    metrics: SequencerMetrics,
    protected val loggerFactory: NamedLoggerFactory,
    memberValidator: SequencerMemberValidator,
)(implicit val closeContext: CloseContext, tracer: Tracer)
    extends BlockUpdateGenerator
    with NamedLogging
    with Spanning {
  import BlockUpdateGenerator.*
  import BlockUpdateGeneratorImpl.*

  private val epsilon = synchronizerSyncCryptoApi.staticSynchronizerParameters.topologyChangeDelay

  private val blockChunkProcessor =
    new BlockChunkProcessor(
      protocolVersion,
      synchronizerSyncCryptoApi,
      sequencerId,
      rateLimitManager,
      orderingTimeFixMode,
      loggerFactory,
      metrics,
      memberValidator = memberValidator,
    )

  override type InternalState = State

  override def internalStateFor(state: BlockEphemeralState): InternalState = State(
    lastBlockTs = state.latestBlock.lastTs,
    lastChunkTs = state.latestBlock.lastTs,
    latestSequencerEventTimestamp = state.latestBlock.latestSequencerEventTimestamp,
    inFlightAggregations = state.inFlightAggregations,
  )

  override def extractBlockEvents(tracedBlock: Traced[RawLedgerBlock]): Traced[BlockEvents] =
    withSpan("BlockUpdateGenerator.extractBlockEvents") { blockTraceContext => _ =>
      val block = tracedBlock.value

      val ledgerBlockEvents = block.events.mapFilter { tracedEvent =>
        withSpan("BlockUpdateGenerator.extractBlockEvents") { implicit traceContext => _ =>
          logger.trace("Extracting event from raw block")
          // TODO(i29003): Defer decompression to addSnapshotsAndValidateSubmissions
          val maxBytesToDecompress = MaxBytesToDecompress.HardcodedDefault
          LedgerBlockEvent.fromRawBlockEvent(protocolVersion, maxBytesToDecompress)(
            tracedEvent.value
          ) match {
            case Left(error) =>
              InvalidLedgerEvent.Error(block.blockHeight, error).discard
              None

            case Right(event) =>
              sequencingTimeLowerBoundExclusive match {
                case Some(boundExclusive)
                    if !LogicalUpgradeTime.canProcessKnowingPastUpgrade(
                      upgradeTime = Some(boundExclusive),
                      sequencingTime = event.timestamp,
                    ) =>
                  SequencedBeforeOrAtLowerBound
                    .Error(event.timestamp, boundExclusive, event.toString)
                    .log()
                  None

                case _ => Some(Traced(event))
              }
          }
        }(tracedEvent.traceContext, tracer)
      }

      Traced(
        BlockEvents(
          block.blockHeight,
          CantonTimestamp.assertFromLong(block.baseSequencingTimeMicrosFromEpoch),
          ledgerBlockEvents,
          tickTopology = block.tickTopologyAtMicrosFromEpoch.map { case (micros, broadcast) =>
            TickTopology(
              CantonTimestamp.assertFromLong(micros),
              (if (broadcast) Left(AllMembersOfSynchronizer)
               else Right(SequencersOfSynchronizer)),
            )
          },
        )
      )(blockTraceContext)
    }(tracedBlock.traceContext, tracer)

  override def chunkBlock(
      blockEvents: BlockEvents
  )(implicit traceContext: TraceContext): immutable.Iterable[BlockChunk] = {
    val blockHeight = blockEvents.height
    metrics.block.height.updateValue(blockHeight)

    val tickChunk =
      if (useTimeProofsToObserveEffectiveTime)
        blockEvents.tickTopology.map { case TickTopology(micros, recipient) =>
          TopologyTickChunk(blockHeight, micros, recipient)
        }
      else Some(MaybeTopologyTickChunk(blockHeight))

    // We must start a new chunk whenever the chunk processing advances lastSequencerEventTimestamp,
    //  otherwise the logic for retrieving a topology snapshot or traffic state could deadlock.

    val chunks = IterableUtil
      .splitAfter(blockEvents.events)(event => isAddressingSequencers(event.value))
      .zipWithIndex
      .map { case (events, index) =>
        NextChunk(blockHeight, index, events)
      } ++ tickChunk ++ Seq(EndOfBlock(blockHeight))

    logger.debug(
      s"Chunked block $blockHeight into ${chunks.size} data chunks and ${tickChunk.toList.size} topology tick chunks"
    )

    chunks
  }

  private def isAddressingSequencers(event: LedgerBlockEvent): Boolean =
    event match {
      case Send(_, signedOrderingRequest, _, _) =>
        val allRecipients =
          signedOrderingRequest.content.batch.allRecipients
        allRecipients.contains(AllMembersOfSynchronizer) ||
        allRecipients.contains(SequencersOfSynchronizer)
      case _ => false
    }

  override final def processBlockChunk(state: InternalState, chunk: BlockChunk)(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[(InternalState, OrderedBlockUpdate)] =
    chunk match {
      case EndOfBlock(height) =>
        val newState = state.copy(lastBlockTs = state.lastChunkTs)
        val update = CompleteBlockUpdate(
          BlockInfo(height, state.lastChunkTs, state.latestSequencerEventTimestamp)
        )
        logger.debug(s"Block $height completed with update $update")
        FutureUnlessShutdown.pure(newState -> update)
      case NextChunk(height, index, chunksEvents) =>
        blockChunkProcessor.processDataChunk(state, height, index, chunksEvents)
      case TopologyTickChunk(blockHeight, tickAtLeastAt, groupRecipient) =>
        blockChunkProcessor.emitTick(state, blockHeight, tickAtLeastAt, groupRecipient)
      case MaybeTopologyTickChunk(blockHeight) =>
        val (activeTopologyTimestamps, pendingTopologyTimestamps) = state.pendingTopologyTimestamps
          .partition(_ + epsilon < state.lastBlockTs.immediateSuccessor)
        val newState = state.copy(pendingTopologyTimestamps = pendingTopologyTimestamps)

        activeTopologyTimestamps.maxOption match {
          // if there is a pending potential topology transaction whose sequencing timestamp is after the activation time of the
          // most recent newly active topology transaction in this block, it acts as a tick too, so no need for a dedicated tick event
          case Some(timestamp) if !pendingTopologyTimestamps.exists(_ > timestamp + epsilon) =>
            blockChunkProcessor.emitTick(
              newState,
              blockHeight,
              timestamp,
              Left(AllMembersOfSynchronizer),
            )
          case _ => FutureUnlessShutdown.pure((newState, ChunkUpdate.noop))
        }
    }
}

object BlockUpdateGeneratorImpl {

  private[block] final case class State(
      lastBlockTs: CantonTimestamp,
      lastChunkTs: CantonTimestamp,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      inFlightAggregations: InFlightAggregations,
      // The sequencing times of potential topology transactions that are not yet effective
      pendingTopologyTimestamps: Vector[CantonTimestamp] = Vector.empty,
  )

  private[update] final case class SequencedValidatedSubmission(
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SignedSubmissionRequest,
      orderingSequencerId: SequencerId,
      topologyOrSequencingSnapshot: SyncCryptoApi,
      topologyTimestampError: Option[SequencerDeliverError],
      consumeTraffic: SubmissionRequestValidator.TrafficConsumption,
      errorOrResolvedGroups: Either[SubmissionOutcome, Map[GroupRecipient, Set[Member]]],
  )(val traceContext: TraceContext)
}
