// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions

import cats.syntax.functor.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.SegmentStatus
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable

class PreviousEpochsRetransmissionsTracker(
    howManyEpochsToKeep: Int,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private val previousEpochs = new mutable.HashMap[EpochNumber, Seq[CommitCertificate]]()

  def endEpoch(epochNumber: EpochNumber, commitCertificates: Seq[CommitCertificate]): Unit = {
    previousEpochs(epochNumber) = commitCertificates
    previousEpochs.remove(EpochNumber(epochNumber - howManyEpochsToKeep)).discard
  }

  def processRetransmissionsRequest(
      epochStatus: ConsensusStatus.EpochStatus
  )(implicit traceContext: TraceContext): Seq[CommitCertificate] =
    previousEpochs.get(epochStatus.epochNumber) match {
      case None =>
        logger.info(
          s"Got a retransmission request for too old or future epoch ${epochStatus.epochNumber}, ignoring"
        )
        Seq.empty
      case Some(previousEpochCommitCertificates) =>
        logger.info(
          s"Got a retransmission request from ${epochStatus.from} for a previous epoch ${epochStatus.epochNumber}"
        )

        val segments: Seq[SegmentStatus] = epochStatus.segments

        val segmentIndexToCommitCerts: Map[Int, Seq[CommitCertificate]] = {
          val numberOfSegments = segments.size
          def segmentIndex(blockIndex: Int) = blockIndex % numberOfSegments
          previousEpochCommitCertificates.zipWithIndex
            .groupBy { case (_, blockIndex) => segmentIndex(blockIndex) }
            .fmap(_.map(_._1))
        }

        segments.zipWithIndex
          .flatMap {
            case (SegmentStatus.Complete, _) => Seq.empty
            case (status: SegmentStatus.Incomplete, segmentIndex) =>
              val segmentCommitCerts =
                segmentIndexToCommitCerts.getOrElse(segmentIndex, Seq.empty)
              status.areBlocksComplete.zip(segmentCommitCerts).collect {
                case (isComplete, commitCertificate) if !isComplete => commitCertificate
              }
          }
          .sortBy(_.prePrepare.message.blockMetadata.blockNumber)
    }

}
