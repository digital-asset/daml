// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.data.Validated
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  PrepareCertificate,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.ViewChange

class ViewChangeMessageValidator(
    membership: Membership,
    segmentBlockNumbers: Seq[BlockNumber],
) {
  def validateViewChangeMessage(viewChange: ViewChange): Either[String, Unit] = {
    val certs = viewChange.consensusCerts

    // these have been validated at an earlier step
    val epochNumber = viewChange.blockMetadata.epochNumber
    val currentViewNumber = viewChange.viewNumber

    for {
      _ <- {
        val wrongEpochs = certs
          .map(_.prePrepare.blockMetadata.epochNumber)
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
          .map(_.prePrepare.blockMetadata.blockNumber)
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
          .groupBy(_.prePrepare.blockMetadata)
          .collect {
            case (blockMetadata, certsForSameBlock) if certsForSameBlock.size > 1 =>
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
          certs.map(_.prePrepare.viewNumber).filter(_ >= currentViewNumber).toSet
        Either.cond(
          viewNumbersInTheFuture.isEmpty,
          (),
          s"there are consensus certificate pre-prepares with view numbers (${viewNumbersInTheFuture
              .mkString(", ")}) higher than or at current view number $currentViewNumber",
        )
      }
      _ <- certs
        .map(validateConsensusCertificate(currentViewNumber, _))
        .sequence_
        .toEither
        .leftMap(_.mkString(", "))
    } yield ()
  }

  private def validateConsensusCertificate(
      currentViewNumber: ViewNumber,
      consensusCertificate: ConsensusCertificate,
  ): Validated[NonEmpty[Seq[String]], Unit] = {
    val prePrepare = consensusCertificate.prePrepare
    val blockNumber = prePrepare.blockMetadata.blockNumber
    val strongQuorum = membership.orderingTopology.strongQuorum
    val (messages, messageName) = consensusCertificate match {
      case prepareCertificate: PrepareCertificate =>
        (prepareCertificate.prepares, "prepare")
      case commitCertificate: CommitCertificate =>
        (commitCertificate.commits, "commit")
    }

    val valid: Validated[NonEmpty[Seq[String]], Unit] = Validated.valid(())
    def invalid(msg: String): Validated[NonEmpty[Seq[String]], Unit] =
      Validated.invalid(NonEmpty(Seq, msg))

    (NonEmpty.from(messages) match {
      case Some(nonEmptyMessages) =>
        val messagesViewNumberValidation = {
          val byViewNumber = nonEmptyMessages.groupBy(_.viewNumber)
          if (byViewNumber.size > 1)
            invalid(
              s"all ${messageName}s should be of the same view number, but they are distributed across multiple view numbers (${byViewNumber.keys
                  .mkString(", ")})"
            )
          else {
            val messagesViewNumber = byViewNumber.head1._1
            if (messagesViewNumber >= currentViewNumber) {
              invalid(
                s"${messageName}s have view number $messagesViewNumber but it should be less than current view number $currentViewNumber"
              )
            } else valid
          }
        }

        val bySender = nonEmptyMessages.groupBy(_.from)

        val repeatedMessageValidation = bySender
          .find(_._2.size > 1)
          .fold(valid) { case (from, prepares) =>
            invalid(
              s"there are more than one ${messageName}s (${prepares.size}) from the same sender $from"
            )
          }

        val wrongBlockNumbersValidation = {
          val wrongBlockNumbers =
            messages.map(_.blockMetadata.blockNumber).filter(_ != blockNumber).toSet
          if (wrongBlockNumbers.isEmpty) valid
          else
            invalid(
              s"there are ${messageName}s for the wrong block number (${wrongBlockNumbers.mkString(", ")})"
            )
        }

        val quorumValidation = {
          val quorumSize = bySender.size
          if (quorumSize < strongQuorum)
            invalid(
              s"expected at least $strongQuorum ${messageName}s, but only got ${bySender.size}"
            )
          else valid
        }

        val hashesValidation = {
          val hash = prePrepare.hash
          messages
            .find(_.hash != hash)
            .fold(valid) { msgWithWrongHash =>
              val from = msgWithWrongHash.from
              invalid(
                s"$messageName from $from has non-matching hash"
              )
            }
        }

        List(
          messagesViewNumberValidation,
          repeatedMessageValidation,
          wrongBlockNumbersValidation,
          quorumValidation,
          hashesValidation,
        ).sequence_
      case None =>
        invalid(s"there are no ${messageName}s")
    }).leftMap(errors =>
      NonEmpty.mk(
        Seq,
        s"$messageName certificate for block $blockNumber has the following errors: ${errors.mkString(", ")}",
      )
    )
  }
}
