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
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusStatus
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusStatus.RetransmissionResult
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
      case messagesStored: PbftMessagesStored =>
        processMessagesStored(messagesStored)
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

  private def processMessagesStored(pbftMessagesStored: PbftMessagesStored)(implicit
      traceContext: TraceContext
  ): Seq[ProcessResult] = {
    val blockIndex = segment.relativeBlockIndex(pbftMessagesStored.blockMetadata.blockNumber)
    val blocks = pbftBlocks(pbftMessagesStored.viewNumber)
    val block = blocks(blockIndex)
    pbftMessagesStored match {
      case _: PrePrepareStored =>
        block.confirmPrePrepareStored()
        block.advance()
      case _: PreparesStored =>
        block.confirmPreparesStored()
        block.advance()
      case _: NewViewStored =>
        blocks.forgetNE.flatMap { block =>
          block.confirmPrePrepareStored()
          block.advance()
        }
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

  def status: ConsensusStatus.SegmentStatus =
    if (isSegmentComplete) ConsensusStatus.SegmentStatus.Complete
    else if (inViewChange)
      ConsensusStatus.SegmentStatus.InViewChange(
        currentViewNumber,
        viewChangeMessagesPresent = viewChangeState
          .get(currentViewNumber)
          .map(_.viewChangeMessageReceivedStatus)
          .getOrElse(Seq.empty),
        segment.slotNumbers.map(isBlockComplete),
      )
    else
      ConsensusStatus.SegmentStatus.InProgress(
        currentViewNumber,
        pbftBlocks
          .get(currentViewNumber)
          .map { blocks =>
            blocks.map(_.status).forgetNE
          }
          .getOrElse(Seq.empty),
      )

  def messagesToRetransmit(
      from: SequencerId,
      remoteStatus: ConsensusStatus.SegmentStatus.Incomplete,
  )(implicit
      traceContext: TraceContext
  ): RetransmissionResult =
    if (remoteStatus.viewNumber > currentViewNumber) {
      logger.debug(
        s"Node $from is in view ${remoteStatus.viewNumber}, which is higher than our current view $currentViewNumber, so we can't help with retransmissions"
      )
      RetransmissionResult.empty
    } else if (inViewChange) {
      // if we are in a view change, we help others make progress to complete the view change
      val vcState = viewChangeState(currentViewNumber)
      val msgsToRetransmit = remoteStatus match {
        case status if (status.viewNumber < currentViewNumber) =>
          // if remote node is in an earlier view change, retransmit all view change messages we have
          vcState.viewChangeMessagesToRetransmit(Seq.empty)
        case ConsensusStatus.SegmentStatus.InViewChange(_, remoteVcMsgs, _) =>
          // if remote node is in the same view change, retransmit view change messages we have that they don't
          vcState.viewChangeMessagesToRetransmit(remoteVcMsgs)
        case _ =>
          // if they've completed the view change, we don't need to do anything (they are ahead of us)
          Seq.empty
      }
      // we do not retransmit commit certs in this case, since most relevant commit certs shall eventually be included
      // in the new-view message when the view change completes
      RetransmissionResult(msgsToRetransmit)
    } else {
      val localBlockStates = pbftBlocks(currentViewNumber)

      remoteStatus match {
        // remote node is making progress on the same view, so we send them what we can to help complete blocks
        case ConsensusStatus.SegmentStatus.InProgress(viewNumber, remoteBlocksStatuses)
            if viewNumber == currentViewNumber =>
          localBlockStates
            .zip(remoteBlocksStatuses)
            .collect { case (localBlockState, inProgress: ConsensusStatus.BlockStatus.InProgress) =>
              (localBlockState, inProgress) // only look at blocks they haven't completed yet
            }
            .foldLeft(RetransmissionResult.empty) {
              case (RetransmissionResult(msgs, ccs), (localBlockState, remoteBlockStatus)) =>
                localBlockState.consensusCertificate match {
                  case Some(cc: CommitCertificate) =>
                    // TODO(#18788): just send a few commits in cases that's enough for remote node to complete quorum
                    RetransmissionResult(msgs, cc +: ccs)
                  case _ =>
                    val newMsgs = msgs ++ localBlockState.messagesToRetransmit(remoteBlockStatus)
                    RetransmissionResult(newMsgs, ccs)
                }
            }

        // remote node is either is a previous view, or in the same view but in an unfinished view change that we've completed.
        // so we give them the new-view message and all messages we have for blocks they haven't completed yet
        case _ =>
          val newView = viewChangeState(currentViewNumber).newViewMessage.toList
          val remoteBlockStatusNoPreparesOrCommits = {
            val allMissing = Seq.fill(membership.sortedPeers.size)(false)
            ConsensusStatus.BlockStatus.InProgress(
              prePrepared = true,
              preparesPresent = allMissing,
              commitsPresent = allMissing,
            )
          }
          localBlockStates
            .zip(remoteStatus.areBlocksComplete)
            .collect {
              case (localBlockState, isRemoteComplete) if !isRemoteComplete => localBlockState
            }
            .foldLeft(RetransmissionResult(newView)) {
              case (RetransmissionResult(msgs, ccs), localBlockState) =>
                localBlockState.consensusCertificate match {
                  case Some(cc: CommitCertificate) =>
                    // TODO(#18788): rethink commit certs here, considering that some certs will be in the new-view message.
                    // we could either: exclude sending commit certs that are already in the new-view,
                    // not take that into account and just send commit certs regardless (which means we may send the same cert twice),
                    // or not send any certs at all considering that the new-view message will likely contain most if not all of them
                    RetransmissionResult(msgs, cc +: ccs)
                  case _ =>
                    val newMsgs =
                      localBlockState.messagesToRetransmit(remoteBlockStatusNoPreparesOrCommits)
                    RetransmissionResult(msgs ++ newMsgs, ccs)
                }
            }
      }
    }

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

  private final class ViewChangeAction(
      condition: (ViewNumber, PbftViewChangeState) => TraceContext => Boolean,
      action: (ViewNumber, PbftViewChangeState) => TraceContext => Seq[ProcessResult],
  ) {
    def run(viewNumber: ViewNumber, state: PbftViewChangeState)(implicit
        traceContext: TraceContext
    ): Seq[ProcessResult] =
      if (condition(viewNumber, state)(traceContext)) {
        action(viewNumber, state)(traceContext)
      } else {
        Seq.empty
      }
  }

  private def viewChangeAction(
      condition: (ViewNumber, PbftViewChangeState) => TraceContext => Boolean
  )(
      action: (ViewNumber, PbftViewChangeState) => TraceContext => Seq[ProcessResult]
  ): ViewChangeAction = new ViewChangeAction(condition, action)

  private val startViewChangeAction = viewChangeAction { case (viewNumber, _) =>
    _ =>
      val hasStartedThisViewChange = currentViewNumber >= viewNumber
      !hasStartedThisViewChange
  } { case (viewNumber, viewState) =>
    implicit traceContext =>
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
  }

  private val startNestedViewChangeTimerAction = viewChangeAction { case (_, viewState) =>
    _ => !strongQuorumReachedForCurrentView && viewState.reachedStrongQuorum
  } { case (viewNumber, _) =>
    _ =>
      strongQuorumReachedForCurrentView = true
      Seq(
        ViewChangeStartNestedTimer(viewChangeBlockMetadata, viewNumber)
      )
  }

  private val createNewViewAction = viewChangeAction { case (viewNumber, viewState) =>
    _ =>
      val thisViewChangeIsInProgress = currentViewNumber == viewNumber && inViewChange
      viewState.shouldCreateNewView && thisViewChangeIsInProgress
  } { case (_, viewState) =>
    _ =>
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
  }

  private val completeViewChangeAction = viewChangeAction { case (viewNumber, _) =>
    _ =>
      val thisViewChangeIsInProgress = currentViewNumber == viewNumber && inViewChange
      thisViewChangeIsInProgress
  } { case (_, viewState) =>
    implicit traceContext =>
      viewState.newViewMessage match {
        case Some(value) => completeViewChange(value)
        case None => Seq.empty
      }
  }

  private def advanceViewChange(
      viewNumber: ViewNumber
  )(implicit traceContext: TraceContext): Seq[ProcessResult] = {
    val viewState = viewChangeState(viewNumber)

    // Note that each result (startViewChange, startNestedViewChangeTimer, createNewView, completeViewChange)
    // should occur at most once per view number
    // TODO(#16820): add validation that each result is only executed (true) once per viewNumber
    Seq(
      startViewChangeAction,
      startNestedViewChangeTimerAction,
      createNewViewAction,
      completeViewChangeAction,
    ).flatMap { action =>
      action.run(viewNumber, viewState)
    }
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
