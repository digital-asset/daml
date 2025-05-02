// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.syntax.bifunctor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.RetransmissionsMessage.{
  RetransmissionRequest,
  RetransmissionResponse,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.{
  BlockStatus,
  SegmentStatus,
}

class RetransmissionMessageValidator(epoch: Epoch) {
  private val currentEpochNumber = epoch.info.number
  private val commitCertValidator = new ConsensusCertificateValidator(
    epoch.currentMembership.orderingTopology.strongQuorum
  )

  private val numberOfNodes = epoch.currentMembership.sortedNodes.size
  private val segments = epoch.segments

  def validateRetransmissionRequest(request: RetransmissionRequest): Either[String, Unit] = {
    val from = request.from
    val status = request.epochStatus
    val validateNumberOfSegments = Either.cond(
      segments.sizeIs == status.segments.size,
      (),
      s"Got a retransmission request from $from with ${status.segments.size} segments when there should be ${segments.size}, ignoring",
    )

    val validatesNeedsResponse = {
      val allComplete = status.segments.forall {
        case SegmentStatus.Complete => true
        case _ => false
      }
      Either.cond(
        !allComplete,
        (),
        s"Got a retransmission request from $from where all segments are complete so no need to process request, ignoring",
      )
    }

    def validateSegmentStatus(
        segmentAndIndex: (Segment, Int),
        segmentStatus: SegmentStatus,
    ): Either[String, Unit] = {
      val (segment, index) = segmentAndIndex
      val numberOfBlocksInSegment = segment.slotNumbers.size
      (segmentStatus match {
        case SegmentStatus.Complete => Right(())
        case SegmentStatus.InViewChange(_, vcs, blocks) =>
          for {
            _ <- Either.cond(vcs.sizeIs == numberOfNodes, (), s"wrong size of view-change list")
            _ <- Either.cond(
              blocks.sizeIs == numberOfBlocksInSegment,
              (),
              s"wrong size of block completion list",
            )
          } yield ()
        case SegmentStatus.InProgress(_, blocks) =>
          val allBlocksWellFormed = blocks.forall {
            case BlockStatus.InProgress(_, prepares, commits) =>
              prepares.sizeIs == numberOfNodes && commits.sizeIs == numberOfNodes
            case _ => true
          }
          for {
            _ <- Either.cond(
              blocks.sizeIs == numberOfBlocksInSegment,
              (),
              s"wrong size of blocks status list",
            )
            _ <- Either.cond(allBlocksWellFormed, (), "wrong size of pbft-messages list")
          } yield ()
      }).leftMap(error =>
        s"Got a malformed retransmission request from $from at segment $index, $error, ignoring"
      )
    }

    for {
      _ <- validateNumberOfSegments
      _ <- validatesNeedsResponse
      _ <- segments.zipWithIndex.zip(status.segments).traverse((validateSegmentStatus _).tupled)
    } yield ()
  }

  def validateRetransmissionResponse(
      response: RetransmissionResponse
  ): Either[String, Unit] = for {
    _ <- validateNonEmptyCommitCerts(response)
    _ <- validateRetransmissionsResponseEpochNumber(response)
    _ <- validateBlockNumbers(response)
    _ <- validateCommitCertificates(response)
  } yield ()

  private def validateNonEmptyCommitCerts(
      response: RetransmissionResponse
  ): Either[String, Unit] = {
    val RetransmissionResponse(from, commitCertificates) = response
    if (commitCertificates.nonEmpty) Right(())
    else
      Left(
        s"Got a retransmission response from $from with no commit certificates, ignoring"
      )
  }

  private def validateRetransmissionsResponseEpochNumber(
      response: RetransmissionResponse
  ): Either[String, Unit] = {
    val RetransmissionResponse(from, commitCertificates) = response
    val wrongEpochs =
      commitCertificates
        .map(_.prePrepare.message.blockMetadata.epochNumber)
        .filter(_ != currentEpochNumber)
    Either.cond(
      wrongEpochs.isEmpty,
      (),
      s"Got a retransmission response from $from for wrong epoch(s) ${wrongEpochs.mkString(", ")}, while we're at $currentEpochNumber, ignoring",
    )
  }

  private def validateBlockNumbers(
      response: RetransmissionResponse
  ): Either[String, Unit] = {
    val RetransmissionResponse(from, commitCertificates) = response

    val wrongBlockNumbers =
      commitCertificates
        .map(_.prePrepare.message.blockMetadata.blockNumber)
        .filter(blockNumber =>
          blockNumber < epoch.info.startBlockNumber || blockNumber > epoch.info.lastBlockNumber
        )

    val blocksWithMultipleCommitCerts = commitCertificates
      .groupBy(_.prePrepare.message.blockMetadata.blockNumber)
      .collect {
        case (blockNumber, certs) if certs.sizeIs > 1 => blockNumber
      }

    for {
      _ <- Either.cond(
        wrongBlockNumbers.isEmpty,
        (),
        s"Got a retransmission response from $from with block number(s) outside of epoch $currentEpochNumber: ${wrongBlockNumbers
            .mkString(", ")}, ignoring",
      )
      _ <- Either.cond(
        blocksWithMultipleCommitCerts.isEmpty,
        (),
        s"Got a retransmission response from $from with multiple commit certificates for the following block number(s): ${blocksWithMultipleCommitCerts
            .mkString(", ")}, ignoring",
      )
    } yield ()
  }

  private def validateCommitCertificates(response: RetransmissionResponse): Either[String, Unit] = {
    val RetransmissionResponse(from, commitCertificates) = response
    commitCertificates
      .traverse(commitCertValidator.validateConsensusCertificate)
      .bimap(
        error =>
          s"Got a retransmission response from $from with invalid commit certificate: $error, ignoring",
        _ => (),
      )
  }

}
