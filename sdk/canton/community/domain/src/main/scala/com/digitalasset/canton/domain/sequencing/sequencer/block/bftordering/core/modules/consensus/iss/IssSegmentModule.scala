// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssSegmentModule.BlockCompletionTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.PbftBlockState.{
  CompletedBlock,
  SendPbftMessage,
  StorePrePrepare,
  StorePrepares,
  StoreResult,
  StoreViewChangeMessage,
  ViewChangeCompleted,
  ViewChangeStartNestedTimer,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.EpochInProgress
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.OrderedBlock
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  NewView,
  PbftNestedViewChangeTimeout,
  PbftNormalTimeout,
  PbftSignedNetworkMessage,
  ViewChange,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  ConsensusSegment,
  P2PNetworkOut,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  Env,
  Module,
  ModuleRef,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

/** Handles the PBFT consensus process for one segment of an epoch, either as a leader or as a follower.
  */
class IssSegmentModule[E <: Env[E]](
    epoch: Epoch,
    segmentState: SegmentState,
    metricsAccumulator: EpochMetricsAccumulator,
    storePbftMessages: Boolean,
    epochStore: EpochStore[E],
    clock: Clock,
    cryptoProvider: CryptoProvider[E],
    latestCompletedEpochLastCommits: Seq[SignedMessage[Commit]],
    epochInProgress: EpochInProgress,
    parent: ModuleRef[Consensus.Message[E]],
    availability: ModuleRef[Availability.Message[E]],
    p2pNetworkOut: ModuleRef[P2PNetworkOut.Message],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends Module[E, ConsensusSegment.Message]
    with NamedLogging {

  private val viewChangeTimeoutManager =
    new TimeoutManager[E, ConsensusSegment.Message, BlockNumber](
      loggerFactory,
      BlockCompletionTimeout,
      segmentState.segment.firstBlockNumber,
    )
  private val thisPeer = epoch.membership.myId
  private val areWeOriginalLeaderOfSegment = thisPeer == segmentState.segment.originalLeader

  private val leaderSegmentState: Option[LeaderSegmentState] =
    if (areWeOriginalLeaderOfSegment)
      Some(new LeaderSegmentState(segmentState, epoch, epochInProgress.completedBlocks))
    else None

  private val segmentBlockMetadata =
    BlockMetadata(epoch.info.number, segmentState.segment.firstBlockNumber)

  override protected def receiveInternal(consensusMessage: ConsensusSegment.Message)(implicit
      context: E#ActorContextT[ConsensusSegment.Message],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(consensusMessage)

    consensusMessage match {
      case ConsensusSegment.Start =>
        val (prepares, viewMessages) = SegmentInProgress.rehydrationMessages(
          segmentState.segment,
          epochInProgress,
        )
        (prepares ++ viewMessages.dropRight(1)).foreach { prepare =>
          segmentState.processEvent(PbftSignedNetworkMessage(prepare)).discard
        }
        viewMessages.lastOption.foreach { msg =>
          processPbftEvent(
            PbftSignedNetworkMessage(msg),
            storeMessages =
              false, // we don't want to store again messages as part of rehydration (but we do want to potentially resend)
          )
        }

        logger.info(
          s"Received `Start` message, segment ${segmentState.segment} is complete = ${segmentState.isSegmentComplete}, " +
            s"view change in progress = ${segmentState.isViewChangeInProgress}"
        )
        if (!segmentState.isSegmentComplete && !segmentState.isViewChangeInProgress)
          viewChangeTimeoutManager.scheduleTimeout(
            PbftNormalTimeout(segmentBlockMetadata, segmentState.currentView)
          )

        if (leaderSegmentState.exists(_.moreSlotsToAssign))
          // Ask availability for batches to be ordered if we have slots available.
          initiatePull()

      case ConsensusSegment.ConsensusMessage.BlockProposal(orderingBlock, forEpochNumber) =>
        val logMessage =
          s"$messageType: received block from local availability with batch IDs: ${orderingBlock.proofs
              .map(_.batchId)}"

        leaderSegmentState.foreach { mySegmentState =>
          // Depending on the timing of events, it is possible that Consensus has an outstanding
          // proposal request to Availability when a view change occurs. A completed view change often
          // leads to completed blocks, and even a completed epoch. As a result, Consensus may receive
          // a proposal (in response to a prior request) that it can no longer assign to
          // a slot in the local segment. `moreSlotsToAssign` is designed to detect such scenarios.
          // The proposal will be in this case ignored, which means that Availability will never get an ack
          // for it, so when we start a new epoch and make a new proposal request, we should get the same
          // proposal again.
          if (mySegmentState.moreSlotsToAssign) {
            // Such outstanding proposal (requested before a view change) could end up coming after the epoch changes.
            // In that case we also want to discard it by detecting that this request was not made during the current epoch.
            if (forEpochNumber != epoch.info.number) {
              logger.info(
                s"$logMessage. Ignoring it because it is from epoch $forEpochNumber and we're in epoch ${epoch.info.number}."
              )
            } else if (orderingBlock.proofs.nonEmpty || mySegmentState.isProgressBlocked) {
              logger.debug(s"$logMessage. Starting consensus process.")
              val orderedBlock =
                mySegmentState.assignToSlot(
                  orderingBlock,
                  clock.now,
                  latestCompletedEpochLastCommits,
                )

              val prePrepare = SignedMessage(
                ConsensusSegment.ConsensusMessage.PrePrepare.create(
                  orderedBlock.metadata,
                  viewNumber = ViewNumber.First,
                  clock.now,
                  orderingBlock,
                  orderedBlock.canonicalCommitSet,
                  from = thisPeer,
                ),
                Signature.noSignature, // TODO(#20458) actually sign
              )
              processPbftEvent(PbftSignedNetworkMessage(prePrepare))
            } else {
              logger.debug(
                s"$logMessage. Not using empty block because we are not blocking progress."
              )
              // Re-issue a pull from availability because we have discarded the previous one.
              initiatePull()
            }
          } else {
            logger.info(
              s"$logMessage. Not using block because we can't assign more slots at the moment. Probably because of a view change."
            )
          }

        }

      case pbftEvent: ConsensusSegment.ConsensusMessage.PbftEvent =>
        processPbftEvent(pbftEvent)

      case ConsensusSegment.ConsensusMessage.BlockOrdered(metadata) =>
        leaderSegmentState.foreach(_.confirmCompleteBlockStored(metadata.blockNumber))

      case ConsensusSegment.Internal.OrderedBlockStored(
            orderedBlock: OrderedBlock,
            commits,
            viewNumber,
          ) =>
        val blockNumber = orderedBlock.metadata.blockNumber
        val orderedBatchIds = orderedBlock.batchRefs.map(_.batchId)

        logger.debug(
          s"$messageType: DB stored block w/ ${orderedBlock.metadata} and batches $orderedBatchIds"
        )
        segmentState.confirmCompleteBlockStored(orderedBlock.metadata.blockNumber, viewNumber)

        // TODO(#18741): Move timeout manipulation earlier (before DB confirmation), once CompleteBlock is emitted
        // If the segment is incomplete, push the segment-specific timeout into the future
        if (!segmentState.isSegmentComplete)
          viewChangeTimeoutManager.scheduleTimeout(
            PbftNormalTimeout(segmentBlockMetadata, segmentState.currentView)
          )
        // Else, the segment is complete; cancel timeouts for this segment and accumulate metrics
        else {
          metricsAccumulator.accumulate(
            segmentState.currentView + 1,
            segmentState.commitVotes,
            segmentState.prepareVotes,
          )
          viewChangeTimeoutManager.cancelTimeout()
        }

        // If there are more slots to locally assign in this epoch, ask availability for more batches
        if (areWeOriginalLeaderOfBlock(blockNumber)) {
          val orderedBatchIds = orderedBlock.batchRefs.map(_.batchId)
          if (leaderSegmentState.exists(_.moreSlotsToAssign))
            initiatePull(orderedBatchIds)
          else if (orderedBatchIds.nonEmpty)
            availability.asyncSend(Availability.Consensus.Ack(orderedBatchIds))
        }

        // Some block completion logic is in the parent ISS consensus module,
        //  such as sending the blocks to the output module and keeping track of global epoch progress.
        //
        //  Note that the segment leader modules notifies the parent module only after having acked
        //  the ordered batches to the availability module: this order is important, as it ensures
        //  that the parent module won't complete the epoch, which causes all segment modules to shut down,
        //  before the segment leader module had a chance to ack the batches.
        //  If the ack were lost or delayed, the segment module for the next epoch would get the same proposal again
        //  from availability, introducing duplicates that would be interpreted as malicious by the mediator
        //  (as sequencer deduplication only works through message aggregation, e.g. due to request amplification)
        //  and result in misleading warnings that would also cause test flakes.
        //
        //  We currently assume that Pekko local sends of non-priority messages are transitively causally ordered,
        //  so that the ack is guaranteed to be received by availability before the request for a new proposal
        //  by a concurrent segment leader module (which is created for the new epoch after the ack is sent).
        //  See https://pekko.apache.org/docs/pekko/current/general/message-delivery-reliability.html#ordering-of-local-message-sends
        //  for more information.
        parent.asyncSend(Consensus.ConsensusMessage.BlockOrdered(orderedBlock, commits))

      case ConsensusSegment.ConsensusMessage.CompletedEpoch(epochNumber) =>
        if (epoch.info.number == epochNumber) {
          viewChangeTimeoutManager.cancelTimeout()
          context.stop()
        } else
          logger.warn(
            s"Received a completed epoch message for epoch $epochNumber but we are in epoch ${epoch.info.number}"
          )

      case ConsensusSegment.Internal.AsyncException(e: Throwable) =>
        logAsyncException(e)

      case _ =>
        logger.error(s"Segment submodule should not receive $messageType")
    }
  }

  private def processPbftEvent(
      pbftEvent: ConsensusSegment.ConsensusMessage.PbftEvent,
      storeMessages: Boolean = storePbftMessages,
  )(implicit
      context: E#ActorContextT[ConsensusSegment.Message],
      traceContext: TraceContext,
  ): Unit = {

    val blockComplete = segmentState.isBlockComplete(pbftEvent.blockMetadata.blockNumber)
    val processResults = segmentState.processEvent(pbftEvent)

    def handleStore(store: StoreResult, sendMsg: () => Unit): Unit = store match {
      case StorePrePrepare(prePrepare) =>
        // TODO(#20914): Store before sending.
        sendMsg()
        context.pipeToSelf(epochStore.addPrePrepare(prePrepare)) {
          case Failure(exception) =>
            logAsyncException(exception)
            // We can't send messages back from here as the module might be already stopped.
            None
          case Success(_) =>
            logger.debug(
              s"DB stored pre-prepare w/ ${prePrepare.message.blockMetadata} and batches ${prePrepare.message.block.proofs
                  .map(_.batchId)}"
            )
            None
        }
      case StorePrepares(prepares) =>
        // TODO(#20914): Store before sending.
        sendMsg()
        context.pipeToSelf(epochStore.addPrepares(prepares)) {
          case Failure(exception) =>
            logAsyncException(exception)
            None
          case Success(_) =>
            prepares.headOption.foreach { head =>
              // We assume all prepares are for the same block, so we just need to look at one metadata.
              val metadata = head.message.blockMetadata
              logger.debug(s"DB stored ${prepares.size} prepares w/ $metadata")
            }
            None
        }
      case StoreViewChangeMessage(vcMessage) =>
        // TODO(#20914): Store before sending.
        sendMsg()
        context.pipeToSelf(epochStore.addViewChangeMessage(vcMessage)) {
          case Failure(exception) =>
            logAsyncException(exception)
            None
          case Success(_) =>
            logger.debug(
              s"DB stored ${vcMessage.message match {
                  case _: ViewChange => "view change"
                  case _: NewView => "new view"
                }} for view ${vcMessage.message.viewNumber} and segment ${vcMessage.message.blockMetadata.blockNumber}"
            )
            None
        }
    }

    processResults.foreach {
      case SendPbftMessage(pbftMessage, store) =>
        def sendMessage(): Unit = {
          val peers = epoch.membership.otherPeers
          logger.debug(
            s"Sending PBFT message to ${peers.map(_.toProtoPrimitive)}: $pbftMessage"
          )
          p2pNetworkOut.asyncSend(
            P2PNetworkOut.Multicast(
              P2PNetworkOut.BftOrderingNetworkMessage.ConsensusMessage(pbftMessage),
              to = peers,
            )
          )
        }

        store match {
          case Some(storeResult) if storeMessages && !blockComplete =>
            handleStore(storeResult, () => sendMessage())
          case _ => sendMessage()
        }

      case CompletedBlock(prePrepare, commitMessages, viewNumber) =>
        storeOrderedBlock(prePrepare, commitMessages, viewNumber)

      case ViewChangeStartNestedTimer(blockMetadata, viewNumber) =>
        logger.debug(
          s"View change w/ strong quorum support; starting nested timer; metadata: $blockMetadata, view: $viewNumber"
        )
        viewChangeTimeoutManager.scheduleTimeout(
          PbftNestedViewChangeTimeout(blockMetadata, viewNumber)
        )

      case ViewChangeCompleted(blockMetadata, viewNumber, storeOption) =>
        logger.debug(s"View change completed; metadata: $blockMetadata, view: $viewNumber")
        viewChangeTimeoutManager.scheduleTimeout(PbftNormalTimeout(blockMetadata, viewNumber))
        storeOption.foreach(storeResult => handleStore(storeResult, () => ()))
    }
  }

  private def storeOrderedBlock(
      prePrepare: SignedMessage[ConsensusSegment.ConsensusMessage.PrePrepare],
      commits: Seq[SignedMessage[ConsensusSegment.ConsensusMessage.Commit]],
      viewNumber: ViewNumber,
  )(implicit
      context: E#ActorContextT[ConsensusSegment.Message],
      traceContext: TraceContext,
  ): Unit =
    // Persist ordered block to epochStore and then self-send ack message.
    pipeToSelf(
      epochStore.addOrderedBlock(
        prePrepare,
        commits,
      )
    ) {
      case Failure(exception) => ConsensusSegment.Internal.AsyncException(exception)
      case Success(_) =>
        val orderedBlock = OrderedBlock(
          prePrepare.message.blockMetadata,
          prePrepare.message.block.proofs,
          prePrepare.message.canonicalCommitSet,
        )
        ConsensusSegment.Internal.OrderedBlockStored(orderedBlock, commits, viewNumber)
    }

  private def logAsyncException(exception: Throwable)(implicit traceContext: TraceContext): Unit =
    logger.error(
      s"Exception raised from async consensus message: ${exception.toString}"
    )

  private def areWeOriginalLeaderOfBlock(blockNumber: BlockNumber): Boolean =
    areWeOriginalLeaderOfSegment && segmentState.segment.slotNumbers.contains(blockNumber)

  private def initiatePull(
      ackedBatchIds: Seq[BatchId] = Seq.empty
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug("Consensus requesting new proposal from local availability")
    availability.asyncSend(
      Availability.Consensus.CreateProposal(
        epoch.membership.orderingTopology,
        cryptoProvider,
        epoch.info.number,
        if (ackedBatchIds.nonEmpty) Some(Availability.Consensus.Ack(ackedBatchIds)) else None,
      )
    )
  }
}

object IssSegmentModule {
  val BlockCompletionTimeout: FiniteDuration = 10.seconds
}
