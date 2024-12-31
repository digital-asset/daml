// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block.LedgerBlockEvent.*
import com.digitalasset.canton.domain.block.data.{BlockEphemeralState, BlockInfo}
import com.digitalasset.canton.domain.block.{BlockEvents, LedgerBlockEvent, RawLedgerBlock}
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregations
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.{
  SignedOrderingRequest,
  SignedOrderingRequestOps,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.InvalidLedgerEvent
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/** Exposes functions that take the deserialized contents of a block from a blockchain integration
  * and compute the new [[BlockUpdate]]s.
  *
  * These functions correspond to the following steps in the block processing stream pipeline:
  * 1. Extracting block events from a raw ledger block ([[extractBlockEvents]]).
  * 2. Chunking such block events into either event chunks terminated by a sequencer-addessed event or a block
  *    completion ([[chunkBlock]]).
  * 3. Validating and enriching chunks to yield block updates ([[processBlockChunk]]).
  *
  * In particular, these functions are responsible for the final timestamp assignment of a given submission request.
  * The timestamp assignment works as follows:
  * 1. an initial timestamp is assigned to the submission request by the sequencer that writes it to the ledger
  * 2. each sequencer that reads the block potentially adapts the previously assigned timestamp
  * deterministically via `ensureStrictlyIncreasingTimestamp`
  * 3. this timestamp is used to compute the [[BlockUpdate]]s
  *
  * Reasoning:
  * Step 1 is done so that every sequencer sees the same timestamp for a given event.
  * Step 2 is needed because different sequencers may assign the same timestamps to different events or may not assign
  * strictly increasing timestamps due to clock skews.
  *
  * Invariants:
  * For step 2, we assume that every sequencer observes the same stream of events from the underlying ledger
  * (and especially that events are always read in the same order).
  */
trait BlockUpdateGenerator {
  import BlockUpdateGenerator.*

  type InternalState

  def internalStateFor(state: BlockEphemeralState): InternalState

  def extractBlockEvents(block: RawLedgerBlock): BlockEvents

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
  final case class TopologyTickChunk(blockHeight: Long, tickAtLeastAt: CantonTimestamp)
      extends BlockChunk
  final case class EndOfBlock(blockHeight: Long) extends BlockChunk
}

class BlockUpdateGeneratorImpl(
    synchronizerId: SynchronizerId,
    protocolVersion: ProtocolVersion,
    domainSyncCryptoApi: DomainSyncCryptoClient,
    sequencerId: SequencerId,
    rateLimitManager: SequencerRateLimitManager,
    orderingTimeFixMode: OrderingTimeFixMode,
    metrics: SequencerMetrics,
    protected val loggerFactory: NamedLoggerFactory,
    memberValidator: SequencerMemberValidator,
)(implicit val closeContext: CloseContext)
    extends BlockUpdateGenerator
    with NamedLogging {
  import BlockUpdateGenerator.*
  import BlockUpdateGeneratorImpl.*

  private val blockChunkProcessor =
    new BlockChunkProcessor(
      synchronizerId,
      protocolVersion,
      domainSyncCryptoApi,
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

  override def extractBlockEvents(block: RawLedgerBlock): BlockEvents = {
    val ledgerBlockEvents = block.events.mapFilter { tracedEvent =>
      implicit val traceContext: TraceContext = tracedEvent.traceContext
      LedgerBlockEvent.fromRawBlockEvent(protocolVersion)(tracedEvent.value) match {
        case Left(error) =>
          InvalidLedgerEvent.Error(block.blockHeight, error).discard
          None
        case Right(value) =>
          Some(Traced(value))
      }
    }

    BlockEvents(
      block.blockHeight,
      ledgerBlockEvents,
      tickTopologyAtLeastAt =
        block.tickTopologyAtMicrosFromEpoch.map(CantonTimestamp.assertFromLong),
    )
  }

  override def chunkBlock(
      blockEvents: BlockEvents
  )(implicit traceContext: TraceContext): immutable.Iterable[BlockChunk] = {
    val blockHeight = blockEvents.height
    metrics.block.height.updateValue(blockHeight)

    val tickChunk =
      blockEvents.tickTopologyAtLeastAt.map(TopologyTickChunk(blockHeight, _))

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
      case Send(_, signedOrderingRequest, _) =>
        val allRecipients =
          signedOrderingRequest.signedSubmissionRequest.content.batch.allRecipients
        allRecipients.contains(AllMembersOfDomain) ||
        allRecipients.contains(SequencersOfDomain)
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
      case TopologyTickChunk(blockHeight, tickAtLeastAt) =>
        blockChunkProcessor.emitTick(state, blockHeight, tickAtLeastAt)
    }
}

object BlockUpdateGeneratorImpl {

  private[block] final case class State(
      lastBlockTs: CantonTimestamp,
      lastChunkTs: CantonTimestamp,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      inFlightAggregations: InFlightAggregations,
  )

  private[update] final case class SequencedSubmission(
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SignedOrderingRequest,
      topologyOrSequencingSnapshot: SyncCryptoApi,
      topologyTimestampError: Option[SequencerDeliverError],
  )(val traceContext: TraceContext)
}
