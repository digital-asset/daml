// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.{
  CommitCertificate as ProtoCommitCertificate,
  ConsensusCertificate as ProtoConsensusCertificate,
  PrepareCertificate as ProtoPrepareCertificate,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
  Prepare,
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
    ProtoPrepareCertificate.of(Some(prePrepare.toProto), sortedPrepares.map(_.toProto))
}

final case class CommitCertificate(
    override val prePrepare: SignedMessage[PrePrepare],
    commits: Seq[SignedMessage[Commit]],
) extends ConsensusCertificate {
  private lazy val sortedCommits: Seq[SignedMessage[Commit]] = commits.sorted

  def toProto: ProtoCommitCertificate =
    ProtoCommitCertificate.of(Some(prePrepare.toProto), sortedCommits.map(_.toProto))
}

object ConsensusCertificate {
  def fromProto(
      consensusCertificate: ProtoConsensusCertificate
  ): ParsingResult[ConsensusCertificate] =
    consensusCertificate.certificate match {
      case ProtoConsensusCertificate.Certificate.Empty =>
        Left(ProtoDeserializationError.OtherError("consensus certificate is empty"))
      case ProtoConsensusCertificate.Certificate.PrepareCertificate(prepareCertificate) =>
        PrepareCertificate.fromProto(prepareCertificate)
      case ProtoConsensusCertificate.Certificate.CommitCertificate(commitCertificate) =>
        CommitCertificate.fromProto(commitCertificate)
    }
}

object PrepareCertificate {
  def fromProto(
      prepareCertificate: v1.PrepareCertificate
  ): ParsingResult[PrepareCertificate] =
    for {
      prePrepare <- ProtoConverter
        .parseRequired(
          SignedMessage.fromProto(v1.ConsensusMessage)(PrePrepare.fromProtoConsensusMessage),
          "prePrepare",
          prepareCertificate.prePrepare,
        )
      prepares <- prepareCertificate.prepares.traverse(
        SignedMessage.fromProto(v1.ConsensusMessage)(Prepare.fromProtoConsensusMessage)
      )
    } yield PrepareCertificate(prePrepare, prepares)
}

object CommitCertificate {
  implicit val ordering: Ordering[Commit] =
    Ordering.by(commit => (commit.from, commit.localTimestamp))

  def fromProto(
      commitCertificate: v1.CommitCertificate
  ): ParsingResult[CommitCertificate] =
    for {
      prePrepare <- ProtoConverter
        .parseRequired(
          SignedMessage.fromProto(v1.ConsensusMessage)(PrePrepare.fromProtoConsensusMessage),
          "prePrepare",
          commitCertificate.prePrepare,
        )
      commits <- commitCertificate.commits.traverse(
        SignedMessage.fromProto(v1.ConsensusMessage)(Commit.fromProtoConsensusMessage)
      )
    } yield CommitCertificate(prePrepare, commits)
}
