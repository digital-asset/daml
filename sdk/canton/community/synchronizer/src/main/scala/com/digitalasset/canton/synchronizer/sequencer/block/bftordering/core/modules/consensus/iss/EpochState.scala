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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch.{
  blockToLeaderFromSegments,
  computeSegments,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Block
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions.EpochStatusBuilder
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
  ConsensusStatus,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.immutable.ListMap

import EpochState.Epoch

class EpochState[E <: Env[E]](
    val epoch: Epoch,
    clock: Clock,
    abort: String => Nothing,
    metrics: BftOrderingMetrics,
    @VisibleForTesting private[iss] val segmentModuleRefFactory: (
        SegmentState,
        EpochMetricsAccumulator,
    ) => E#ModuleRefT[ConsensusSegment.Message],
    completedBlocks: Seq[Block] = Seq.empty,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit
    synchronizerProtocolVersion: ProtocolVersion,
    config: BftBlockOrdererConfig,
    mc: MetricsContext,
) extends NamedLogging
    with FlagCloseable {

  private val metricsAccumulator = new EpochMetricsAccumulator()
  def emitEpochStats(metrics: BftOrderingMetrics, nextEpoch: EpochInfo): Unit =
    IssConsensusModuleMetrics.emitEpochStats(
      metrics,
      nextEpoch,
      epoch,
      metricsAccumulator.viewsCount,
      metricsAccumulator.discardedMessages,
      metricsAccumulator.retransmittedMessages,
      metricsAccumulator.retransmittedCommitCertificates,
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

  // Segment module references are lazy so that the segment module factory is not triggered on object creation
  private lazy val segmentModules: Map[BftNodeId, E#ModuleRefT[ConsensusSegment.Message]] =
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

  private lazy val mySegmentModule = segmentModules.get(epoch.currentMembership.myId)
  private val mySegment = epoch.segments.find(_.originalLeader == epoch.currentMembership.myId)

  private lazy val blockToSegmentModule: Map[BlockNumber, E#ModuleRefT[ConsensusSegment.Message]] =
    (epoch.info.startBlockNumber to epoch.info.lastBlockNumber).map { n =>
      val blockNumber = BlockNumber(n)
      blockNumber -> segmentModules(epoch.blockToLeader(BlockNumber(blockNumber)))
    }.toMap

  def requestSegmentStatuses()(implicit traceContext: TraceContext): EpochStatusBuilder = {
    epoch.segments.zipWithIndex.foreach { case (segment, segmentIndex) =>
      segmentModules(segment.originalLeader).asyncSend(
        ConsensusSegment.RetransmissionsMessage.StatusRequest(segmentIndex)
      )
    }
    new EpochStatusBuilder(
      epoch.currentMembership.myId,
      epoch.info.number,
      epoch.segments.size,
    )
  }

  def processRetransmissionsRequest(
      epochStatus: ConsensusStatus.EpochStatus
  )(implicit traceContext: TraceContext): Unit =
    synchronizeWithClosingSync("processRetransmissionsRequest") {
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
      from: BftNodeId,
      commitCerts: Seq[CommitCertificate],
  )(implicit traceContext: TraceContext): Unit =
    synchronizeWithClosingSync("processRetransmissionsRequest") {
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
      module.asyncSendNoTrace(ConsensusSegment.Start)
    }

  def confirmBlockCompleted(
      blockMetadata: BlockMetadata,
      commitCertificate: CommitCertificate,
  )(implicit traceContext: TraceContext): Unit = {
    setCommitCertificate(blockMetadata.blockNumber, commitCertificate)
    sendMessageToSegmentModules(
      ConsensusSegment.ConsensusMessage.BlockOrdered(
        blockMetadata,
        isEmpty = commitCertificate.prePrepare.message.block.proofs.isEmpty,
      )
    )
  }

  def notifyEpochCompletionToSegments(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): Unit =
    sendMessageToSegmentModules(ConsensusSegment.ConsensusMessage.CompletedEpoch(epochNumber))

  def notifyEpochCancellationToSegments(epochNumber: EpochNumber)(implicit
      traceContext: TraceContext
  ): Unit =
    sendMessageToSegmentModules(ConsensusSegment.ConsensusMessage.CancelEpoch(epochNumber))

  def localAvailabilityMessageReceived(
      message: Consensus.LocalAvailability
  )(implicit
      traceContext: TraceContext
  ): Unit =
    sendMessageToSegmentModules(ConsensusSegment.ConsensusMessage.LocalAvailability(message))

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
    synchronizeWithClosingSync("handleMessage")(msg match {
      case localAvailability: ConsensusSegment.ConsensusMessage.LocalAvailability =>
        mySegmentModule.foreach(_.asyncSend(localAvailability))
      case pbftEvent: ConsensusSegment.ConsensusMessage.PbftEvent =>
        blockToSegmentModule(pbftEvent.blockMetadata.blockNumber).asyncSend(pbftEvent)
      case ConsensusSegment.ConsensusMessage.CompletedEpoch(_) =>
        segmentModules.values.foreach(_.asyncSend(msg))
      case cancelEpoch: ConsensusSegment.ConsensusMessage.CancelEpoch =>
        segmentModules.values.foreach(_.asyncSend(cancelEpoch))
      case ConsensusSegment.ConsensusMessage.BlockOrdered(block, _) =>
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
  ) {
    val segments: Seq[Segment] =
      computeSegments(currentMembership.leaders, info.startBlockNumber, info.length)

    lazy val blockToLeader: Map[BlockNumber, BftNodeId] = blockToLeaderFromSegments(segments)
  }

  object Epoch {
    // Using `take` below, we select either a subset of leaders (if leaders.size > epoch length),
    // or the entire leaders collection. In general, having leaders.size > epoch length seems like
    // poor parameters configuration.
    def computeSegments(
        leaders: Seq[BftNodeId],
        startBlockNumber: BlockNumber,
        epochLength: EpochLength,
    ): Seq[Segment] =
      leaders
        .take(epochLength.toInt)
        .zipWithIndex
        .map { case (node, index) =>
          computeSegment(leaders, startBlockNumber, epochLength, node, index)
        }

    private def computeSegment(
        leaders: Seq[BftNodeId],
        startBlockNumber: BlockNumber,
        epochLength: EpochLength,
        leader: BftNodeId,
        leaderIndex: Int,
    ): Segment = {
      val firstSlot = BlockNumber(startBlockNumber + leaderIndex)
      val stepSize = leaders.size
      Segment(
        originalLeader = leader,
        slotNumbers = NonEmpty.mk(
          Seq,
          firstSlot,
          LazyList
            .iterate(firstSlot + stepSize)(_ + stepSize)
            .takeWhile(_ < (startBlockNumber + epochLength))
            .map(BlockNumber(_))*
        ),
      )
    }

    def blockToLeaderFromSegments(segments: Seq[Segment]): Map[BlockNumber, BftNodeId] = (for {
      segment <- segments
      number <- segment.slotNumbers
    } yield (number, segment.originalLeader)).toMap

    def blockToLeadersFromEpochInfo(
        leaders: Seq[BftNodeId],
        startBlockNumber: BlockNumber,
        epochLength: EpochLength,
    ): Map[BlockNumber, BftNodeId] =
      blockToLeaderFromSegments(computeSegments(leaders, startBlockNumber, epochLength))
  }

  final case class Segment(
      originalLeader: BftNodeId,
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
