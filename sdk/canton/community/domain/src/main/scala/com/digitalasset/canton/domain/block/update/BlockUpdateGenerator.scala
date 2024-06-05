// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import cats.syntax.functor.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block.LedgerBlockEvent.*
import com.digitalasset.canton.domain.block.data.{
  BlockEphemeralState,
  BlockInfo,
  BlockUpdateEphemeralState,
}
import com.digitalasset.canton.domain.block.{BlockEvents, LedgerBlockEvent, RawLedgerBlock}
import com.digitalasset.canton.domain.metrics.BlockMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.SignedOrderingRequestOps
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError.InvalidLedgerEvent
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
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
  * 4. Signing chunked events with the signing key of the sequencer ([[signChunkEvents]]).
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
  ): FutureUnlessShutdown[(InternalState, OrderedBlockUpdate[UnsignedChunkEvents])]

  def signChunkEvents(events: UnsignedChunkEvents)(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[SignedChunkEvents]
}

object BlockUpdateGenerator {

  type EventsForSubmissionRequest = Map[Member, SequencedEvent[ClosedEnvelope]]

  type SignedEvents = Map[Member, OrdinarySerializedEvent]

  sealed trait BlockChunk extends Product with Serializable
  final case class NextChunk(
      blockHeight: Long,
      chunkIndex: Int,
      events: NonEmpty[Seq[Traced[LedgerBlockEvent]]],
  ) extends BlockChunk
  final case class EndOfBlock(blockHeight: Long) extends BlockChunk
}

class BlockUpdateGeneratorImpl(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    domainSyncCryptoApi: DomainSyncCryptoClient,
    sequencerId: SequencerId,
    maybeLowerTopologyTimestampBound: Option[CantonTimestamp],
    rateLimitManager: SequencerRateLimitManager,
    orderingTimeFixMode: OrderingTimeFixMode,
    metrics: BlockMetrics,
    protected val loggerFactory: NamedLoggerFactory,
    unifiedSequencer: Boolean,
)(implicit val closeContext: CloseContext)
    extends BlockUpdateGenerator
    with NamedLogging {
  import BlockUpdateGenerator.*
  import BlockUpdateGeneratorImpl.*

  private val blockChunkProcessor =
    new BlockChunkProcessor(
      domainId,
      protocolVersion,
      domainSyncCryptoApi,
      sequencerId,
      rateLimitManager,
      orderingTimeFixMode,
      loggerFactory,
      metrics,
    )

  override type InternalState = State

  override def internalStateFor(state: BlockEphemeralState): InternalState = State(
    lastBlockTs = state.latestBlock.lastTs,
    lastChunkTs = state.latestBlock.lastTs,
    latestSequencerEventTimestamp = state.latestBlock.latestSequencerEventTimestamp,
    ephemeral = state.state.toBlockUpdateEphemeralState,
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
    BlockEvents(block.blockHeight, ledgerBlockEvents)
  }

  override def chunkBlock(
      block: BlockEvents
  )(implicit traceContext: TraceContext): immutable.Iterable[BlockChunk] = {
    val blockHeight = block.height
    metrics.height.updateValue(blockHeight)

    // We must start a new chunk whenever the chunk processing advances lastSequencerEventTimestamp
    // Otherwise the logic for retrieving a topology snapshot or traffic state could deadlock
    IterableUtil
      .splitAfter(block.events)(event => isAddressingSequencers(event.value))
      .zipWithIndex
      .map { case (events, index) =>
        NextChunk(blockHeight, index, events)
      } ++ Seq(EndOfBlock(blockHeight))
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
  ): FutureUnlessShutdown[(InternalState, OrderedBlockUpdate[UnsignedChunkEvents])] =
    chunk match {
      case EndOfBlock(height) =>
        val newState = state.copy(lastBlockTs = state.lastChunkTs)
        val update = CompleteBlockUpdate(
          BlockInfo(height, state.lastChunkTs, state.latestSequencerEventTimestamp)
        )
        FutureUnlessShutdown.pure(newState -> update)
      case NextChunk(height, index, chunksEvents) =>
        blockChunkProcessor.processChunk(state, height, index, chunksEvents)
    }

  override def signChunkEvents(unsignedEvents: UnsignedChunkEvents)(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[SignedChunkEvents] = {
    val UnsignedChunkEvents(
      sender,
      events,
      signingSnapshot,
      sequencingTimestamp,
      sequencingSnapshot,
      trafficStates,
      submissionRequestTraceContext,
    ) = unsignedEvents
    implicit val traceContext: TraceContext = submissionRequestTraceContext
    val signingTimestamp = signingSnapshot.ipsSnapshot.timestamp
    val signedEventsF =
      maybeLowerTopologyTimestampBound match {
        case Some(bound) if bound > signingTimestamp =>
          // As the required topology snapshot timestamp is older than the lower topology timestamp bound, the timestamp
          // of this sequencer's very first topology snapshot, tombstone the events. This enables subscriptions to signal to
          // subscribers that this sequencer is not in a position to serve the events behind these sequencer counters.
          // Comparing against the lower signing timestamp bound prevents tombstones in "steady-state" sequencing beyond
          // "soon" after initial sequencer onboarding. See #13609
          events.toSeq.parTraverse { case (member, event) =>
            logger.info(
              s"Sequencing tombstone for ${member.identifier} at ${event.timestamp} and ${event.counter}. Sequencer signing key at $signingTimestamp not available before the bound $bound."
            )
            // sign tombstones using key valid at sequencing timestamp as event timestamp has no signing key and we
            // are not sequencing the event anyway, but the tombstone
            val err = DeliverError.create(
              event.counter,
              sequencingTimestamp, // use sequencing timestamp for tombstone
              domainId,
              MessageId(String73.tryCreate("tombstone")), // dummy message id
              SequencerErrors.PersistTombstone(event.timestamp, event.counter),
              protocolVersion,
            )
            for {
              signedContent <-
                SignedContent
                  .create(
                    sequencingSnapshot.pureCrypto,
                    sequencingSnapshot,
                    err,
                    Some(sequencingSnapshot.ipsSnapshot.timestamp),
                    HashPurpose.SequencedEventSignature,
                    protocolVersion,
                  )
                  .valueOr { syncCryptoError =>
                    ErrorUtil.internalError(
                      new RuntimeException(
                        s"Error signing tombstone deliver error: $syncCryptoError"
                      )
                    )
                  }
            } yield {
              member -> OrdinarySequencedEvent(signedContent, None)(traceContext)
            }
          }
        case _ =>
          events.toSeq
            .parTraverse { case (member, event) =>
              SignedContent
                .create(
                  signingSnapshot.pureCrypto,
                  signingSnapshot,
                  event,
                  None,
                  HashPurpose.SequencedEventSignature,
                  protocolVersion,
                )
                .valueOr(syncCryptoError =>
                  ErrorUtil.internalError(
                    new RuntimeException(s"Error signing events: $syncCryptoError")
                  )
                )
                .map { signedContent =>
                  // only include traffic state for the sender
                  val trafficStateO = Option.when(!unifiedSequencer || member == sender) {
                    trafficStates.getOrElse(
                      member,
                      ErrorUtil.invalidState(s"Sender $member unknown by rate limiter."),
                    )
                  }
                  member ->
                    OrdinarySequencedEvent(signedContent, trafficStateO)(traceContext)
                }
            }
      }
    signedEventsF.map(signedEvents => SignedChunkEvents(signedEvents.toMap))
  }
}

object BlockUpdateGeneratorImpl {

  type SignedEvents = NonEmpty[Map[Member, OrdinarySerializedEvent]]

  type EventsForSubmissionRequest = Map[Member, SequencedEvent[ClosedEnvelope]]

  private[block] final case class State(
      lastBlockTs: CantonTimestamp,
      lastChunkTs: CantonTimestamp,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
      ephemeral: BlockUpdateEphemeralState,
  )

  private[update] final case class SequencedSubmission(
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SignedContent[SubmissionRequest],
      sequencingSnapshot: SyncCryptoApi,
      topologySnapshotO: Option[SyncCryptoApi],
  )(val traceContext: TraceContext)

  private[update] final case class RecipientStats(
      participants: Boolean = false,
      mediators: Boolean = false,
      sequencers: Boolean = false,
      broadcast: Boolean = false,
  ) {

    private[block] def updateMetric(
        sender: Member,
        payloadSize: Int,
        logger: TracedLogger,
        metrics: BlockMetrics,
    )(implicit traceContext: TraceContext): Unit = {
      val messageType = {
        // by looking at the recipient lists and the sender, we'll figure out what type of message we've been getting
        (sender, participants, mediators, sequencers, broadcast) match {
          case (ParticipantId(_), false, true, false, false) =>
            "send-confirmation-response"
          case (ParticipantId(_), true, true, false, false) =>
            "send-confirmation-request"
          case (MediatorId(_), true, false, false, false) =>
            "send-verdict"
          case (ParticipantId(_), true, false, false, false) =>
            "send-commitment"
          case (SequencerId(_), true, false, true, false) =>
            "send-topup"
          case (SequencerId(_), false, true, true, false) =>
            "send-topup-med"
          case (_, false, false, false, true) =>
            "send-topology"
          case (_, false, false, false, false) =>
            "send-time-proof"
          case _ =>
            def r(boolean: Boolean, s: String) = if (boolean) Seq(s) else Seq.empty
            val recipients = r(participants, "participants") ++
              r(mediators, "mediators") ++
              r(sequencers, "sequencers") ++
              r(broadcast, "broadcast")
            logger.warn(s"Unexpected message from $sender to " + recipients.mkString(","))
            "send-unexpected"
        }
      }
      val mc = MetricsContext(
        "sender" -> sender.toString,
        "type" -> messageType,
      )
      metrics.blockEvents.mark()(mc)
      metrics.blockEventBytes.mark(payloadSize.longValue)(mc)
    }
  }
}
