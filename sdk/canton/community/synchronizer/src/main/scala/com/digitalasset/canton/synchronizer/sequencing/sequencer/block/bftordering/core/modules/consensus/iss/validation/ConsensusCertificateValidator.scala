// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.data.Validated
import cats.syntax.foldable.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.ViewNumber
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  PrepareCertificate,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage

class ConsensusCertificateValidator(strongQuorum: Int) {

  private val valid: Validated[NonEmpty[Seq[String]], Unit] = Validated.valid(())
  private def invalid(msg: String): Validated[NonEmpty[Seq[String]], Unit] =
    Validated.invalid(NonEmpty(Seq, msg))

  def validateRetransmittedConsensusCertificate(
      consensusCertificate: ConsensusCertificate
  ): Either[String, Unit] =
    validateConsensusCertificate(None, consensusCertificate).toEither

  def validateNewViewConsensusCertificate(
      currentViewNumber: ViewNumber,
      consensusCertificate: ConsensusCertificate,
  ): Validated[NonEmpty[Seq[String]], Unit] =
    validateConsensusCertificate(Some(currentViewNumber), consensusCertificate)
      .leftMap(e => NonEmpty.mk(Seq, e))

  private def validateConsensusCertificate(
      currentViewNumber: Option[ViewNumber],
      consensusCertificate: ConsensusCertificate,
  ): Validated[String, Unit] = {
    val prePrepare = consensusCertificate.prePrepare
    val blockNumber = prePrepare.message.blockMetadata.blockNumber
    val (messages, messageName) = consensusCertificate match {
      case prepareCertificate: PrepareCertificate =>
        (
          prepareCertificate.prepares: Seq[SignedMessage[ConsensusMessage.PbftNormalCaseMessage]],
          "prepare",
        )
      case commitCertificate: CommitCertificate =>
        (
          commitCertificate.commits: Seq[SignedMessage[ConsensusMessage.PbftNormalCaseMessage]],
          "commit",
        )
    }

    (NonEmpty.from(messages) match {
      case Some(nonEmptyMessages) =>
        val messagesViewNumberValidation = {
          val byViewNumber = nonEmptyMessages.groupBy(_.message.viewNumber)
          if (byViewNumber.sizeIs > 1)
            invalid(
              s"all ${messageName}s should be of the same view number, but they are distributed across multiple view numbers (${byViewNumber.keys
                  .mkString(", ")})"
            )
          else
            currentViewNumber.fold(valid) { viewNumber =>
              val messagesViewNumber = byViewNumber.head1._1
              if (messagesViewNumber >= viewNumber) {
                invalid(
                  s"${messageName}s have view number $messagesViewNumber but it should be less than current view number $viewNumber"
                )
              } else valid
            }
        }

        val bySender = nonEmptyMessages.groupBy(_.from)

        val repeatedMessageValidation = bySender
          .find(_._2.sizeIs > 1)
          .fold(valid) { case (from, prepares) =>
            invalid(
              s"there are more than one ${messageName}s (${prepares.size}) from the same sender $from"
            )
          }

        val wrongBlockNumbersValidation = {
          val wrongBlockNumbers =
            messages.map(_.message.blockMetadata.blockNumber).filter(_ != blockNumber).toSet
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
          val hash = prePrepare.message.hash
          messages
            .find(_.message.hash != hash)
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
      s"$messageName certificate for block $blockNumber has the following errors: ${errors.mkString(", ")}"
    )
  }
}
