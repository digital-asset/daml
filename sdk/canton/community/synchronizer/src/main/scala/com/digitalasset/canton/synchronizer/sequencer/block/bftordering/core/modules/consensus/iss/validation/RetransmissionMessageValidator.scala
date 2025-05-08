// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.syntax.bifunctor.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.RetransmissionMessageValidator.RetransmissionResponseValidationError
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
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
  ): Either[RetransmissionResponseValidationError, Unit] = for {
    certs <- validateNonEmptyCommitCerts(response)
    _ <- validateRetransmissionsResponseEpochNumber(response.from, certs)
    _ <- validateBlockNumbers(response)
    _ <- validateCommitCertificates(response)
  } yield ()

  private def validateNonEmptyCommitCerts(
      response: RetransmissionResponse
  ): Either[RetransmissionResponseValidationError, NonEmpty[Seq[CommitCertificate]]] = {
    val RetransmissionResponse(from, commitCertificates) = response
    NonEmpty.from(commitCertificates) match {
      case Some(nel) => Right(nel)
      case None =>
        Left(RetransmissionResponseValidationError.MalformedMessage(from, "no commit certificates"))
    }
  }

  private def validateRetransmissionsResponseEpochNumber(
      from: BftNodeId,
      commitCertificates: NonEmpty[Seq[CommitCertificate]],
  ): Either[RetransmissionResponseValidationError, Unit] = {
    val byEpoch = commitCertificates
      .groupBy(_.prePrepare.message.blockMetadata.epochNumber)

    if (byEpoch.sizeIs > 1)
      Left(
        RetransmissionResponseValidationError
          .MalformedMessage(
            from,
            s"commit certificates from different epochs ${byEpoch.keys.toSeq.sorted.mkString(", ")}",
          )
      )
    else if (!byEpoch.contains(currentEpochNumber))
      Left(
        RetransmissionResponseValidationError.WrongEpoch(
          from,
          messageEpochNumber = byEpoch.head1._1,
          currentEpochNumber = currentEpochNumber,
        )
      )
    else Right(())
  }

  private def validateBlockNumbers(
      response: RetransmissionResponse
  ): Either[RetransmissionResponseValidationError, Unit] = {
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

    val result = for {
      _ <- Either.cond(
        wrongBlockNumbers.isEmpty,
        (),
        s"block number(s) outside of epoch $currentEpochNumber: ${wrongBlockNumbers.mkString(", ")}",
      )
      _ <- Either.cond(
        blocksWithMultipleCommitCerts.isEmpty,
        (),
        s"multiple commit certificates for the following block number(s): ${blocksWithMultipleCommitCerts
            .mkString(", ")}",
      )
    } yield ()

    result.leftMap(RetransmissionResponseValidationError.MalformedMessage(from, _))
  }

  private def validateCommitCertificates(
      response: RetransmissionResponse
  ): Either[RetransmissionResponseValidationError, Unit] = {
    val RetransmissionResponse(from, commitCertificates) = response
    commitCertificates
      .traverse(commitCertValidator.validateConsensusCertificate)
      .bimap(
        error =>
          RetransmissionResponseValidationError
            .MalformedMessage(from, s"invalid commit certificate: $error"),
        _ => (),
      )
  }

}

object RetransmissionMessageValidator {
  sealed trait RetransmissionResponseValidationError {
    def errorMsg: String
  }
  object RetransmissionResponseValidationError {
    final case class WrongEpoch(
        from: BftNodeId,
        messageEpochNumber: EpochNumber,
        currentEpochNumber: EpochNumber,
    ) extends RetransmissionResponseValidationError {
      val errorMsg =
        s"Got a retransmission response from $from for wrong epoch(s) $messageEpochNumber, while we're at $currentEpochNumber, ignoring"
    }
    final case class MalformedMessage(from: BftNodeId, reason: String)
        extends RetransmissionResponseValidationError {
      val errorMsg =
        s"Got a retransmission response from $from with $reason, ignoring"
    }
  }
}
