// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.data.Validated
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  NewView,
  ViewChange,
}

class ViewChangeMessageValidator(
    membership: Membership,
    segmentBlockNumbers: Seq[BlockNumber],
) {
  private val strongQuorum = membership.orderingTopology.strongQuorum
  private val consensusCertificateValidator = new ConsensusCertificateValidator(strongQuorum)

  private val valid: Validated[NonEmpty[Seq[String]], Unit] = Validated.valid(())
  private def invalid(msg: String): Validated[NonEmpty[Seq[String]], Unit] =
    Validated.invalid(NonEmpty(Seq, msg))

  def validateViewChangeMessage(viewChange: ViewChange): Either[String, Unit] = {
    val certs = viewChange.consensusCerts

    // these have been validated at an earlier step
    val epochNumber = viewChange.blockMetadata.epochNumber
    val currentViewNumber = viewChange.viewNumber

    for {
      _ <- {
        val wrongEpochs = certs
          .map(_.prePrepare.message.blockMetadata.epochNumber)
          .filter(_ != epochNumber)
          .toSet
        Either.cond(
          wrongEpochs.isEmpty,
          (),
          s"there are consensus certs for the wrong epoch (${wrongEpochs.mkString(", ")})",
        )
      }
      _ <- {
        val wrongSegmentBlocks = certs
          .map(_.prePrepare.message.blockMetadata.blockNumber)
          .filter(blockNumber => !segmentBlockNumbers.contains(blockNumber))
          .toSet
        Either.cond(
          wrongSegmentBlocks.isEmpty,
          (),
          s"there are consensus certs for blocks from the wrong segment (${wrongSegmentBlocks
              .mkString(", ")})",
        )
      }
      _ <- {
        val blocksWithRepeatedCerts = certs
          .groupBy(_.prePrepare.message.blockMetadata)
          .collect {
            case (blockMetadata, certsForSameBlock) if certsForSameBlock.sizeIs > 1 =>
              blockMetadata.blockNumber
          }
          .toSet
        Either.cond(
          blocksWithRepeatedCerts.isEmpty,
          (),
          s"there are more than one consensus certificates for the following blocks (${blocksWithRepeatedCerts
              .mkString(", ")})",
        )
      }
      _ <- {
        val viewNumbersInTheFuture =
          certs.map(_.prePrepare.message.viewNumber).filter(_ >= currentViewNumber).toSet
        Either.cond(
          viewNumbersInTheFuture.isEmpty,
          (),
          s"there are consensus certificate pre-prepares with view numbers (${viewNumbersInTheFuture
              .mkString(", ")}) higher than or at current view number $currentViewNumber",
        )
      }
      _ <- certs
        .map(
          consensusCertificateValidator.validateNewViewConsensusCertificate(currentViewNumber, _)
        )
        .sequence_
        .toEither
        .leftMap(_.mkString(", "))
    } yield ()
  }

  def validateNewViewMessage(newView: NewView): Either[String, Unit] = {
    val currentViewNumber = newView.viewNumber
    val segmentNumber = newView.blockMetadata.blockNumber
    val epochNumber = newView.blockMetadata.epochNumber

    for {
      _ <- {
        val wrongEpochs = newView.viewChanges
          .map(_.message.blockMetadata.epochNumber)
          .filter(_ != epochNumber)
          .toSet
        Either.cond(
          wrongEpochs.isEmpty,
          (),
          s"there are view change messages for the wrong epoch (${wrongEpochs.mkString(", ")} instead of $epochNumber)",
        )
      }
      _ <- {
        val wrongSegments = newView.viewChanges
          .map(_.message.blockMetadata.blockNumber)
          .filter(_ != segmentNumber)
          .toSet
        Either.cond(
          wrongSegments.isEmpty,
          (),
          s"there are view change messages for the wrong segment identifier (${wrongSegments
              .mkString(", ")} instead of $segmentNumber)",
        )
      }
      _ <- {
        val wrongViewNumbers = newView.viewChanges
          .map(_.message.viewNumber)
          .filter(_ != currentViewNumber)
          .toSet
        Either.cond(
          wrongViewNumbers.isEmpty,
          (),
          s"there are view change messages for the wrong view (${wrongViewNumbers
              .mkString(", ")} instead of $currentViewNumber)",
        )
      }
      _ <- {
        val viewChangesWithRepeatedSender = newView.viewChanges
          .groupBy(_.message.from)
          .collect {
            case (from, viewChangesFromSameSender) if viewChangesFromSameSender.sizeIs > 1 =>
              from
          }
          .toSet
        Either.cond(
          viewChangesWithRepeatedSender.isEmpty,
          (),
          s"there are more than one view change messages from the same sender for the following nodes: ${viewChangesWithRepeatedSender
              .mkString(", ")}",
        )
      }
      _ <- {
        val quorumSize = newView.viewChanges.size
        Either.cond(
          quorumSize == strongQuorum,
          (),
          s"expected $strongQuorum view-change messages, but got $quorumSize",
        )
      }
      _ <- newView.viewChanges
        .map(_.message)
        .traverse_(viewChange =>
          validateViewChangeMessage(viewChange)
            .leftMap(e => s"view change message from ${viewChange.from} is invalid: $e")
        )
      _ <- {
        val blockNumbers = newView.prePrepares.map(_.message.blockMetadata.blockNumber)
        Either.cond(
          blockNumbers == segmentBlockNumbers,
          (),
          s"expected pre-prepares to be for blocks (in-order) ${segmentBlockNumbers
              .mkString(", ")} but instead they were for ${blockNumbers.mkString(", ")}",
        )
      }
      _ <- {
        val wrongEpochs = newView.prePrepares
          .map(_.message.blockMetadata.epochNumber)
          .filter(_ != epochNumber)
          .toSet
        Either.cond(
          wrongEpochs.isEmpty,
          (),
          s"there are pre-prepares for the wrong epoch (${wrongEpochs.mkString(", ")} instead of $epochNumber)",
        )
      }
      _ <- {
        val definedPrePrepares = newView.computedCertificatePerBlock.fmap(_.prePrepare.message)
        newView.prePrepares
          .map(_.message)
          .map { prePrepare =>
            definedPrePrepares.get(prePrepare.blockMetadata.blockNumber) match {
              case Some(expectedPrePrepare) =>
                if (expectedPrePrepare == prePrepare) valid
                else
                  invalid(
                    s"pre-prepare for block ${prePrepare.blockMetadata.blockNumber} does not match the one expected from consensus certificate"
                  )
              case None =>
                if (prePrepare.block.proofs.nonEmpty)
                  invalid(
                    s"pre-prepare for block ${prePrepare.blockMetadata.blockNumber} should be for bottom block, but it contains proofs of availability"
                  )
                else if (prePrepare.viewNumber != currentViewNumber)
                  invalid(
                    s"pre-prepare for bottom block ${prePrepare.blockMetadata.blockNumber} should be for view $currentViewNumber but it is for ${prePrepare.viewNumber}"
                  )
                else valid
            }
          }
          .sequence_
          .toEither
          .leftMap(_.mkString(", "))
      }
    } yield ()
  }
}
