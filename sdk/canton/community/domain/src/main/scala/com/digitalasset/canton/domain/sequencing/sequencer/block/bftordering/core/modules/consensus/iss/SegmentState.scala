// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.metrics.BftOrderingMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Segment
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.PbftBlockState.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.SegmentState.computeLeaderOfView
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class SegmentState(
    val segment: Segment,
    val epochNumber: EpochNumber,
    val membership: Membership,
    eligibleLeaders: Seq[SequencerId],
    clock: Clock,
    completedBlocks: Seq[Block],
    abort: String => Nothing,
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit mc: MetricsContext)
    extends NamedLogging {

  private val originalLeaderIndex = eligibleLeaders.indexOf(segment.originalLeader)
  private val viewChangeBlockMetadata = BlockMetadata(epochNumber, segment.slotNumbers.head1)

  // Only one view is active at a time, starting at view=0, inViewChange=false
  // - Upon view change start, due to timeout or >= f+1 peer votes, increment currentView and inViewChange=true
  // - Upon view change completion, inViewChange=false (and view stays the same)

  private var currentLeader: SequencerId = segment.originalLeader
  private var currentViewNumber: ViewNumber = ViewNumber.First
  private var inViewChange: Boolean = false
  private var strongQuorumReachedForCurrentView: Boolean = false

  private val futureViewMessagesQueue = mutable.Queue[SignedMessage[PbftNormalCaseMessage]]()
  private val viewChangeState = new mutable.HashMap[ViewNumber, PbftViewChangeState]

  private val pbftBlocks = new mutable.HashMap[ViewNumber, NonEmpty[Seq[PbftBlockState]]]()
  pbftBlocks
    .put(
      currentViewNumber,
      segment.slotNumbers.map(blockNumber =>
        completedBlocks
          .find(_.blockNumber == blockNumber)
          .fold[PbftBlockState](
            new PbftBlockState.InProgress(
              membership,
              clock,
              currentLeader,
              epochNumber,
              currentViewNumber,
              abort,
              metrics,
              loggerFactory,
            )
          )(block =>
            new PbftBlockState.AlreadyOrdered(
              currentLeader,
              block.commitCertificate,
              loggerFactory,
            )
          )
      ),
    )
    .discard

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def processEvent(
      event: PbftEvent
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    val blockCompletion = blockCompletionState
    val processResults = event match {
      case PbftSignedNetworkMessage(signedMessage) =>
        signedMessage.message match {
          case _: PbftNormalCaseMessage =>
            processNormalCaseMessage(
              signedMessage.asInstanceOf[SignedMessage[PbftNormalCaseMessage]]
            )
          case _: PbftViewChangeMessage =>
            processViewChangeMessage(
              signedMessage.asInstanceOf[SignedMessage[PbftViewChangeMessage]]
            )
        }
      case timeout: PbftTimeout =>
        processTimeout(timeout)
    }
    // if the block has been completed in a previous view and it gets completed again as a result of a view change
    // in a higher view, we don't want to signal again that the block got completed
    processResults.filter {
      case c: CompletedBlock
          if blockCompletion(
            segment.relativeBlockIndex(c.prePrepare.message.blockMetadata.blockNumber)
          ) =>
        false
      case _ => true
    }
  }

  def confirmCompleteBlockStored(blockNumber: BlockNumber, viewNumber: ViewNumber): Unit =
    pbftBlocks(viewNumber)(segment.relativeBlockIndex(blockNumber))
      .confirmCompleteBlockStored()

  def isBlockComplete(blockNumber: BlockNumber): Boolean =
    findViewWhereBlockIsComplete(blockNumber).isDefined

  def isSegmentComplete: Boolean = blockCompletionState.forall(identity)

  def blockCommitMessages(blockNumber: BlockNumber): Seq[SignedMessage[Commit]] = {
    val viewNumber = findViewWhereBlockIsComplete(blockNumber)
      .getOrElse(abort(s"Block $blockNumber should have been completed"))
    pbftBlocks(viewNumber)(segment.relativeBlockIndex(blockNumber)).commitMessageQuorum
  }

  def currentView: ViewNumber = currentViewNumber

  def prepareVotes: Map[SequencerId, Long] = sumOverInProgressBlocks(_.prepareVoters)

  def commitVotes: Map[SequencerId, Long] = sumOverInProgressBlocks(_.commitVoters)

  def leader: SequencerId = currentLeader

  private def sumOverInProgressBlocks(
      getVoters: InProgress => Iterable[SequencerId]
  ): Map[SequencerId, Long] = {
    val inProgressBlocks =
      pbftBlocks.values.flatten.collect {
        case block: InProgress => // AlreadyOrdered blocks are loaded at restart and don't have stats
          block
      }
    val votes =
      inProgressBlocks
        .flatMap(getVoters)
        .groupBy(identity)
        .view
        .mapValues(_.size.toLong)
        .toMap
    votes
  }

  // Normal Case: PrePrepare, Prepare, Commit
  // Note: We may want to limit the number of messages in the future queue, per peer
  //       When capacity is reached, we can (a) drop new messages, or (b) evict older for newer
  private def processNormalCaseMessage(
      msg: SignedMessage[PbftNormalCaseMessage]
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    var result = Seq.empty[ProcessResult]
    if (msg.message.viewNumber < currentViewNumber)
      logger.info(
        s"Segment received PbftNormalCaseMessage with stale view ${msg.message.viewNumber}; " +
          s"current view = $currentViewNumber"
      )
    else if (msg.message.viewNumber > currentViewNumber || inViewChange) {
      futureViewMessagesQueue.enqueue(msg)
      logger.info(
        s"Segment received early PbftNormalCaseMessage; message view = ${msg.message.viewNumber}, " +
          s"current view = $currentViewNumber, inViewChange = $inViewChange"
      )
    } else
      result = processPbftNormalCaseMessage(msg, msg.message.blockMetadata.blockNumber)
    result
  }

  // View Change Case: ViewChange, NewView
  // Note: Similarly to the future message queue, we may want to limit how many concurrent viewChangeState
  //       entries exist in the map at any given point in time
  private def processViewChangeMessage(
      msg: SignedMessage[PbftViewChangeMessage]
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    var result = Seq.empty[ProcessResult]
    if (msg.message.viewNumber < currentViewNumber)
      logger.info(
        s"Segment received PbftViewChangeMessage with stale view ${msg.message.viewNumber}; " +
          s"current view = $currentViewNumber"
      )
    else if (msg.message.viewNumber == currentViewNumber && !inViewChange)
      logger.info(
        s"Segment received PbftViewChangeMessage with matching view ${msg.message.viewNumber}, " +
          s"but View Change is already complete, current view = $currentViewNumber"
      )
    else {
      val vcState = viewChangeState.getOrElseUpdate(
        msg.message.viewNumber,
        new PbftViewChangeState(
          membership,
          computeLeader(msg.message.viewNumber),
          epochNumber,
          msg.message.viewNumber,
          segment.slotNumbers,
          metrics,
          loggerFactory,
        ),
      )
      if (vcState.processMessage(msg) && vcState.shouldAdvanceViewChange) {
        result = advanceViewChange(msg.message.viewNumber)
      }
    }
    result
  }

  private def processTimeout(
      timeout: PbftTimeout
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    var result = Seq.empty[ProcessResult]
    if (timeout.viewNumber < currentViewNumber)
      logger.info(
        s"Segment received PbftTimeout with stale view ${timeout.viewNumber}; " +
          s"current view = $currentViewNumber"
      )
    else if (timeout.viewNumber > currentViewNumber)
      abort(
        s"Segment should not receive timeout from future view ${timeout.viewNumber}; " +
          s"current view = $currentViewNumber"
      )
    else {
      val nextViewNumber = ViewNumber(timeout.viewNumber + 1)
      viewChangeState
        .getOrElseUpdate(
          nextViewNumber,
          new PbftViewChangeState(
            membership,
            computeLeader(nextViewNumber),
            epochNumber,
            nextViewNumber,
            segment.slotNumbers,
            metrics,
            loggerFactory,
          ),
        )
        .discard
      result = advanceViewChange(nextViewNumber)
    }
    result
  }

  private def blockCompletionState: Seq[Boolean] =
    (0 until segment.slotNumbers.size).map(idx => isBlockComplete(segment.slotNumbers(idx)))

  private def computeLeader(viewNumber: ViewNumber): SequencerId =
    computeLeaderOfView(viewNumber, originalLeaderIndex, eligibleLeaders)

  private def findViewWhereBlockIsComplete(blockNumber: BlockNumber): Option[ViewNumber] = {
    val blockIndex = segment.relativeBlockIndex(blockNumber)
    (ViewNumber.First to currentViewNumber)
      .find(n => pbftBlocks.get(ViewNumber(n)).exists(blocks => blocks(blockIndex).isBlockComplete))
      .map(ViewNumber(_))
  }

  @VisibleForTesting
  private[iss] def isViewChangeInProgress: Boolean = inViewChange

  @VisibleForTesting
  private[iss] def futureQueueSize: Int = futureViewMessagesQueue.size

  private def advanceViewChange(
      viewNumber: ViewNumber
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    val viewState = viewChangeState(viewNumber)

    // Note that each result (startViewChange, startNestedViewChangeTimer, createNewView, completeViewChange)
    // should occur at most once per view number
    // TODO(#16820): add validation that each result is only executed (true) once per viewNumber

    val hasStartedThisViewChange = currentViewNumber >= viewNumber
    val startViewChangeResult =
      if (!hasStartedThisViewChange) {
        currentViewNumber = viewNumber
        currentLeader = computeLeader(viewNumber)
        inViewChange = true
        strongQuorumReachedForCurrentView = false
        // if we got the new-view message before anything else (common during rehydration),
        // then no need to create a view-change message
        if (viewState.newViewMessage.isDefined) Seq.empty
        else {
          Seq(viewState.viewChangeFromSelf match {
            // if we rehydrated a view-change message from self, we don't need to create or store it again
            case Some(rehydratedViewChangeMessage) =>
              SendPbftMessage(
                rehydratedViewChangeMessage,
                None,
              )
            case None =>
              val viewChangeMessage = startViewChange(viewNumber)
              SendPbftMessage(
                viewChangeMessage,
                Some(StoreViewChangeMessage(viewChangeMessage)),
              )
          })
        }
      } else
        Seq.empty

    val startNestedViewChangeTimerResult =
      if (!strongQuorumReachedForCurrentView && viewState.reachedStrongQuorum) {
        strongQuorumReachedForCurrentView = true
        Seq(
          ViewChangeStartNestedTimer(viewChangeBlockMetadata, viewNumber)
        )
      } else
        Seq.empty

    val thisViewChangeIsInProgress = currentViewNumber == viewNumber && inViewChange
    val createNewViewResult =
      if (viewState.shouldCreateNewView && thisViewChangeIsInProgress) {
        val newViewMessage = viewState
          .createNewViewMessage(
            viewChangeBlockMetadata,
            segmentIdx = originalLeaderIndex,
            clock.now,
          )
        Seq(
          SendPbftMessage(
            newViewMessage,
            Some(StoreViewChangeMessage(newViewMessage)),
          )
        )
      } else
        Seq.empty

    val completeViewChangeResult =
      Option
        .when(thisViewChangeIsInProgress) {
          viewState.newViewMessage.map(completeViewChange)
        }
        .flatten
        .getOrElse(Seq.empty)

    startViewChangeResult ++ startNestedViewChangeTimerResult ++ createNewViewResult ++ completeViewChangeResult
  }

  private def startViewChange(
      newViewNumber: ViewNumber
  )(implicit traceContext: TraceContext): SignedMessage[ViewChange] = {
    val viewChangeMessage = {
      val initialAccumulator =
        Seq.fill[Option[ConsensusCertificate]](segment.slotNumbers.size)(None)
      val consensusCerts =
        // for each slot, find the highest view from which there exists Some(ConsensusCertificate),
        // starting at newView-1 and moving all the way down to view=0, default to None if no such certificate exists
        (ViewNumber.First until newViewNumber).reverse.foldLeft(initialAccumulator) {
          case (acc, view) =>
            pbftBlocks.get(ViewNumber(view)).fold(acc) { blocks =>
              blocks.zip(acc).map {
                case (_, Some(cert)) => Some(cert)
                case (block, None) => block.consensusCertificate
              }
            }
        }
      SignedMessage(
        ViewChange.create(
          viewChangeBlockMetadata,
          segmentIndex = originalLeaderIndex,
          newViewNumber,
          clock.now,
          consensusCerts = consensusCerts.collect { case Some(cert) => cert },
          from = membership.myId,
        ),
        Signature.noSignature,
      )
    }

    viewChangeState(newViewNumber)
      .processMessage(viewChangeMessage)
      .discard

    viewChangeMessage
  }

  private def completeViewChange(
      newView: SignedMessage[NewView]
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    val blockToCommitCert: Map[BlockNumber, CommitCertificate] =
      newView.message.computedCertificatePerBlock.collect {
        case (blockNumber, cc: CommitCertificate) =>
          (blockNumber, cc)
      }
    val blockToPrePrepare =
      newView.message.prePrepares.groupBy(_.message.blockMetadata.blockNumber).collect {
        case (blockNumber, Seq(prePrepare)) =>
          (blockNumber, prePrepare)
        case _ =>
          abort(
            "There should be exactly one PrePrepare for each slot upon completing a view change"
          )
      }

    // Create the new set of blocks for the currentView
    pbftBlocks
      .put(
        currentViewNumber,
        segment.slotNumbers.map { blockNumber =>
          blockToCommitCert.get(blockNumber) match {
            case Some(cc: CommitCertificate) =>
              new AlreadyOrdered(currentLeader, cc, loggerFactory)
            case _ =>
              new PbftBlockState.InProgress(
                membership,
                clock,
                currentLeader,
                epochNumber,
                currentViewNumber,
                abort,
                metrics,
                loggerFactory,
              )
          }
        },
      )
      .discard

    // End the active view change
    inViewChange = false

    // Process previously queued messages that are now relevant for the current view.
    // It is important that this step happens before processing the pre-prepares for the new-view so that
    // during rehydration we can first process previously stored prepares and thus avoid that new conflicting prepares
    // are created as a result of rehydrating the new-view message's pre-prepares.
    val queuedMessages =
      futureViewMessagesQueue.dequeueAll(_.message.viewNumber == currentViewNumber)
    val futureMessageQueueResults =
      for {
        pbftMessage <- queuedMessages
        processResult <- processPbftNormalCaseMessage(
          pbftMessage,
          pbftMessage.message.blockMetadata.blockNumber,
        )
      } yield processResult

    // Call process (and advance) to bootstrap each block with the PrePrepare included in the NewView message
    val postViewChangeResults =
      for {
        (blockNumber, prePrepare) <- blockToPrePrepare.toList
          .sortBy(_._1)
          // filtering out completed blocks from commit certificates since there is no more need to process these pre-prepares
          .filterNot(kv => blockToCommitCert.contains(kv._1))
        processResult <- processPbftNormalCaseMessage(prePrepare, blockNumber)
      } yield processResult

    // Return the full set of ProcessResults, starting with ViewChangeCompleted
    val completeViewChangeResult = Seq(
      ViewChangeCompleted(
        viewChangeBlockMetadata,
        currentViewNumber,
        Option.when(newView.from != membership.myId)(StoreViewChangeMessage(newView)),
      )
    )

    val completedBlockResults = blockToCommitCert.values.map(cc =>
      CompletedBlock(cc.prePrepare, cc.commits, currentViewNumber)
    )

    completeViewChangeResult ++ completedBlockResults ++ postViewChangeResults ++ futureMessageQueueResults
  }

  private def processPbftNormalCaseMessage(
      pbftNormalCaseMessage: SignedMessage[PbftNormalCaseMessage],
      blockNumber: BlockNumber,
  )(implicit
      traceContext: TraceContext
  ): Seq[ProcessResult] = {
    val blockIndex = segment.relativeBlockIndex(blockNumber)
    if (pbftBlocks(currentViewNumber)(blockIndex).processMessage(pbftNormalCaseMessage)) {
      pbftBlocks(currentViewNumber)(blockIndex).advance()
    } else {
      Seq.empty
    }
  }
}

object SegmentState {

  def computeLeaderOfView(
      viewNumber: ViewNumber,
      originalLeaderIndex: Int,
      eligibleLeaders: Seq[SequencerId],
  ): SequencerId = {
    val newIndex = (originalLeaderIndex + viewNumber) % eligibleLeaders.size
    eligibleLeaders(newIndex.toInt)
  }
}
