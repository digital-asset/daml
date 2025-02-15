// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import cats.syntax.traverse.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.leaders.LeaderSelectionPolicy
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions.EpochStatusBuilder
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  ConsensusSegment,
  ConsensusStatus,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.immutable.ListMap

import EpochState.Epoch

class EpochState[E <: Env[E]](
    val epoch: Epoch,
    clock: Clock,
    abort: String => Nothing,
    metrics: BftOrderingMetrics,
    @VisibleForTesting val segmentModuleRefFactory: (
        SegmentState,
        EpochMetricsAccumulator,
    ) => E#ModuleRefT[ConsensusSegment.Message],
    completedBlocks: Seq[Block] = Seq.empty,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit mc: MetricsContext)
    extends NamedLogging
    with FlagCloseable {

  private val metricsAccumulator = new EpochMetricsAccumulator()
  def emitEpochStats(metrics: BftOrderingMetrics, nextEpoch: EpochInfo): Unit =
    IssConsensusModuleMetrics.emitEpochStats(
      metrics,
      nextEpoch,
      epoch,
      metricsAccumulator.viewsCount,
      metricsAccumulator.prepareVotes,
      metricsAccumulator.commitVotes,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var lastBlockCommitMessagesOption: Option[Seq[SignedMessage[Commit]]] = None

  private val commitCertificates =
    Array.fill[Option[CommitCertificate]](epoch.info.length.toInt)(None)
  completedBlocks.foreach(b => setCommitCertificate(b.blockNumber, b.commitCertificate))

  def lastBlockCommitMessages: Seq[SignedMessage[Commit]] =
    lastBlockCommitMessagesOption.getOrElse(abort("The current epoch's last block is not complete"))

  def epochCompletionStatus: EpochState.EpochCompletionStatus =
    commitCertificates.toList.sequence
      .fold[EpochState.EpochCompletionStatus](EpochState.Incomplete)(EpochState.Complete(_))

  private val segmentModules: Map[SequencerId, E#ModuleRefT[ConsensusSegment.Message]] =
    ListMap.from(epoch.segments.view.map { segment =>
      segment.originalLeader -> segmentModuleRefFactory(
        new SegmentState(
          segment,
          epoch,
          clock,
          completedBlocks,
          abort,
          metrics,
          loggerFactory,
        ),
        metricsAccumulator,
      )
    })

  private val mySegmentModule = segmentModules.get(epoch.currentMembership.myId)
  private val mySegment = epoch.segments.find(_.originalLeader == epoch.currentMembership.myId)

  private val blockToSegmentModule: Map[BlockNumber, E#ModuleRefT[ConsensusSegment.Message]] = {
    val blockToLeader = (for {
      segment <- epoch.segments
      number <- segment.slotNumbers
    } yield (number, segment.originalLeader)).toMap

    (epoch.info.startBlockNumber to epoch.info.lastBlockNumber).map { n =>
      val blockNumber = BlockNumber(n)
      blockNumber -> segmentModules(blockToLeader(BlockNumber(blockNumber)))
    }.toMap
  }

  def requestSegmentStatuses(): EpochStatusBuilder = {
    epoch.segments.zipWithIndex.foreach { case (segment, segmentIndex) =>
      segmentModules(segment.originalLeader).asyncSend(
        ConsensusSegment.RetransmissionsMessage.StatusRequest(segmentIndex)
      )
    }
    new EpochStatusBuilder(epoch.currentMembership.myId, epoch.info.number, epoch.segments.size)
  }

  def processRetransmissionsRequest(
      epochStatus: ConsensusStatus.EpochStatus
  )(implicit traceContext: TraceContext): Unit =
    performUnlessClosing("processRetransmissionsRequest") {
      epoch.segments.zip(epochStatus.segments).foreach { case (segment, segmentStatus) =>
        segmentStatus match {
          case status: ConsensusStatus.SegmentStatus.Incomplete =>
            segmentModules(segment.originalLeader).asyncSend(
              ConsensusSegment.RetransmissionsMessage
                .RetransmissionRequest(epochStatus.from, status)
            )
          case _ => ()
        }
      }
    }.onShutdown {
      logger.info(
        s"At epoch ${epoch.info.number} received message after shutdown, so discarding retransmission request from ${epochStatus.from}"
      )
    }

  def processRetransmissionResponse(
      from: SequencerId,
      commitCerts: Seq[CommitCertificate],
  )(implicit traceContext: TraceContext): Unit =
    performUnlessClosing("processRetransmissionsRequest") {
      commitCerts.foreach { cc =>
        blockToSegmentModule(cc.prePrepare.message.blockMetadata.blockNumber)
          .asyncSend(ConsensusSegment.ConsensusMessage.RetransmittedCommitCertificate(from, cc))
      }
    }.onShutdown {
      logger.info(
        s"At epoch ${epoch.info.number} received message after shutdown, so discarding retransmission response from $from"
      )
    }

  def startSegmentModules(): Unit =
    segmentModules.foreach { case (_, module) =>
      module.asyncSend(ConsensusSegment.Start)
    }

  def confirmBlockCompleted(
      blockMetadata: BlockMetadata,
      commitCertificate: CommitCertificate,
  )(implicit traceContext: TraceContext): Unit = {
    setCommitCertificate(blockMetadata.blockNumber, commitCertificate)
    sendMessageToSegmentModules(ConsensusSegment.ConsensusMessage.BlockOrdered(blockMetadata))
  }

  def notifyEpochCompletionToSegments(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): Unit =
    sendMessageToSegmentModules(ConsensusSegment.ConsensusMessage.CompletedEpoch(epochNumber))

  def notifyEpochCancellationToSegments(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): Unit =
    sendMessageToSegmentModules(ConsensusSegment.ConsensusMessage.CancelEpoch(epochNumber))

  def proposalCreated(orderingBlock: OrderingBlock, epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): Unit =
    sendMessageToSegmentModules(
      ConsensusSegment.ConsensusMessage.BlockProposal(orderingBlock, epochNumber)
    )

  def processPbftMessage(event: ConsensusSegment.ConsensusMessage.PbftEvent)(implicit
      traceContext: TraceContext
  ): Unit = sendMessageToSegmentModules(event)

  private def setCommitCertificate(
      blockNumber: BlockNumber,
      commitCertificate: CommitCertificate,
  ): Unit = {
    val blockIndex = epoch.info.relativeBlockIndex(blockNumber)
    commitCertificates(blockIndex) = Some(commitCertificate)

    if (blockNumber == epoch.info.lastBlockNumber)
      lastBlockCommitMessagesOption = Some(commitCertificate.commits)
  }

  private def sendMessageToSegmentModules(
      msg: ConsensusSegment.ConsensusMessage
  )(implicit traceContext: TraceContext): Unit =
    performUnlessClosing("handleMessage")(msg match {
      case proposalCreated: ConsensusSegment.ConsensusMessage.BlockProposal =>
        mySegmentModule.foreach(_.asyncSend(proposalCreated))
      case pbftEvent: ConsensusSegment.ConsensusMessage.PbftEvent =>
        blockToSegmentModule(pbftEvent.blockMetadata.blockNumber).asyncSend(pbftEvent)
      case ConsensusSegment.ConsensusMessage.CompletedEpoch(_) =>
        segmentModules.values.foreach(_.asyncSend(msg))
      case cancelEpoch: ConsensusSegment.ConsensusMessage.CancelEpoch =>
        segmentModules.values.foreach(_.asyncSend(cancelEpoch))
      case ConsensusSegment.ConsensusMessage.BlockOrdered(block) =>
        (for {
          segment <- mySegment
          // only send block completion for blocks we're not a leader of
          // because we already keep track of completion for our own blocks
          if !segment.slotNumbers.contains(block.blockNumber)
          module <- mySegmentModule
        } yield module)
          // the segment submodule whose segment we're the leader of needs to keep track of block completion for all segments
          // in order to figure out whether it is blocking epoch progress and thus should use empty blocks
          .foreach(_.asyncSend(msg))
    }).onShutdown {
      logger.info(
        s"At epoch ${epoch.info.number} received message after shutdown, so discarding $msg"
      )
    }
}

object EpochState {

  final case class Epoch(
      info: EpochInfo,
      currentMembership: Membership,
      previousMembership: Membership,
      leaderSelectionPolicy: LeaderSelectionPolicy,
  ) {

    // If leaders.size > epoch length, not every leader can be assigned a segment. Thus, we rotate them
    // to provide more fairness.
    val leaders: Seq[SequencerId] = {
      val selectedLeaders =
        leaderSelectionPolicy.selectLeaders(currentMembership.orderingTopology.peers)
      if (selectedLeaders.sizeIs > info.length.toInt) {
        leaderSelectionPolicy.rotateLeaders(selectedLeaders, info.number)
      } else selectedLeaders.toSeq
    }

    // Using `take` below, we select either a subset of leaders (if leaders.size > epoch length),
    // or the entire leaders collection. In general, having leaders.size > epoch length seems like
    // poor parameters configuration.
    val segments: Seq[Segment] = leaders
      .take(info.length.toInt)
      .zipWithIndex
      .map { case (peer, index) =>
        createSegment(peer, index)
      }

    private def createSegment(leader: SequencerId, leaderIndex: Int): Segment = {
      val firstSlot = BlockNumber(info.startBlockNumber + leaderIndex)
      val stepSize = leaders.size
      Segment(
        originalLeader = leader,
        slotNumbers = NonEmpty.mk(
          Seq,
          firstSlot,
          LazyList
            .iterate(firstSlot + stepSize)(_ + stepSize)
            .takeWhile(_ < info.startOfNextEpochBlockNumber)
            .map(BlockNumber(_))*
        ),
      )
    }
  }

  final case class Segment(
      originalLeader: SequencerId,
      slotNumbers: NonEmpty[Seq[BlockNumber]],
  ) {
    def relativeBlockIndex(blockNumber: BlockNumber): Int = slotNumbers.indexOf(blockNumber)
    def firstBlockNumber: BlockNumber = slotNumbers.head1
    def isFirstInSegment(blockNumber: BlockNumber): Boolean = relativeBlockIndex(blockNumber) == 0
    def previousBlockNumberInSegment(currentBlockNumber: BlockNumber): Option[BlockNumber] =
      // `lift` is just like `get`
      slotNumbers.lift(slotNumbers.indexOf(currentBlockNumber) - 1)
  }

  sealed trait EpochCompletionStatus {
    def isComplete: Boolean
  }
  final case class Complete(commitCertificates: Seq[CommitCertificate])
      extends EpochCompletionStatus {
    override def isComplete: Boolean = true
  }
  case object Incomplete extends EpochCompletionStatus {
    override def isComplete: Boolean = false
  }

}
