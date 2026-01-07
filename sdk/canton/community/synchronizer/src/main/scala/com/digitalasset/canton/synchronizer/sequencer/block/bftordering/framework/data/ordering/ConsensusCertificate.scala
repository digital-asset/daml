// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
  Prepare,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  CommitCertificate as ProtoCommitCertificate,
  ConsensusCertificate as ProtoConsensusCertificate,
  PrepareCertificate as ProtoPrepareCertificate,
}

sealed trait ConsensusCertificate {
  def prePrepare: SignedMessage[PrePrepare]
  def blockMetadata: BlockMetadata = prePrepare.message.blockMetadata
}

final case class PrepareCertificate(
    override val prePrepare: SignedMessage[PrePrepare],
    prepares: Seq[SignedMessage[Prepare]],
) extends ConsensusCertificate {
  private lazy val sortedPrepares: Seq[SignedMessage[Prepare]] = prepares.sorted

  def toProto: ProtoPrepareCertificate =
    ProtoPrepareCertificate(Some(prePrepare.toProtoV1), sortedPrepares.map(_.toProtoV1))
}

final case class CommitCertificate(
    override val prePrepare: SignedMessage[PrePrepare],
    commits: Seq[SignedMessage[Commit]],
) extends ConsensusCertificate {
  private lazy val sortedCommits: Seq[SignedMessage[Commit]] = commits.sorted

  def toProto: ProtoCommitCertificate =
    ProtoCommitCertificate(Some(prePrepare.toProtoV1), sortedCommits.map(_.toProtoV1))
}

object ConsensusCertificate {
  def fromProto(
      consensusCertificate: ProtoConsensusCertificate,
      actualSender: Option[BftNodeId],
  ): ParsingResult[ConsensusCertificate] =
    consensusCertificate.certificate match {
      case ProtoConsensusCertificate.Certificate.Empty =>
        Left(ProtoDeserializationError.OtherError("consensus certificate is empty"))
      case ProtoConsensusCertificate.Certificate.PrepareCertificate(prepareCertificate) =>
        PrepareCertificate.fromProto(prepareCertificate, actualSender)
      case ProtoConsensusCertificate.Certificate.CommitCertificate(commitCertificate) =>
        CommitCertificate.fromProto(commitCertificate, actualSender)
    }
}

object PrepareCertificate {
  def fromProto(
      prepareCertificate: v30.PrepareCertificate,
      actualSender: Option[BftNodeId],
  ): ParsingResult[PrepareCertificate] =
    for {
      prePrepare <- ProtoConverter
        .parseRequired(
          SignedMessage.fromProto(v30.ConsensusMessage)(
            PrePrepare.fromProtoConsensusMessage(actualSender, _)
          ),
          "prePrepare",
          prepareCertificate.prePrepare,
        )
      prepares <- prepareCertificate.prepares.traverse(
        SignedMessage.fromProto(v30.ConsensusMessage)(
          Prepare.fromProtoConsensusMessage(actualSender, _)
        )
      )
    } yield PrepareCertificate(prePrepare, prepares)
}

object CommitCertificate {
  implicit val ordering: Ordering[Commit] =
    Ordering.by(commit => (commit.from, commit.localTimestamp))

  def fromProto(
      commitCertificate: v30.CommitCertificate,
      actualSender: Option[BftNodeId],
  ): ParsingResult[CommitCertificate] =
    for {
      prePrepare <- ProtoConverter
        .parseRequired(
          SignedMessage.fromProto(v30.ConsensusMessage)(
            PrePrepare.fromProtoConsensusMessage(actualSender, _)
          ),
          "prePrepare",
          commitCertificate.prePrepare,
        )
      commits <- commitCertificate.commits.traverse(
        SignedMessage.fromProto(v30.ConsensusMessage)(
          Commit.fromProtoConsensusMessage(actualSender, _)
        )
      )
    } yield CommitCertificate(prePrepare, commits)
}
