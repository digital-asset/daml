// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  CommitCertificate as ProtoCommitCertificate,
  ConsensusCertificate as ProtoConsensusCertificate,
  PrepareCertificate as ProtoPrepareCertificate,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PrePrepare,
  Prepare,
}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

sealed trait ConsensusCertificate {
  def prePrepare: PrePrepare
}

final case class PrepareCertificate(override val prePrepare: PrePrepare, prepares: Seq[Prepare])
    extends ConsensusCertificate {
  private lazy val sortedPrepares: Seq[Prepare] = prepares.sorted

  def toProto: ProtoPrepareCertificate =
    ProtoPrepareCertificate.of(Some(prePrepare.toProto), sortedPrepares.map(_.toProto))
}

final case class CommitCertificate(override val prePrepare: PrePrepare, commits: Seq[Commit])
    extends ConsensusCertificate {
  private lazy val sortedCommits: Seq[Commit] = commits.sorted

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
      prepareCertificate: ProtoPrepareCertificate
  ): ParsingResult[PrepareCertificate] =
    for {
      consensusPP <- ProtoConverter
        .required("prePrepare", prepareCertificate.prePrepare)
      prePrepare <- PrePrepare.fromProtoConsensusMessage(consensusPP)
      prepares <- prepareCertificate.prepares.traverse(Prepare.fromProtoConsensusMessage)
    } yield PrepareCertificate(prePrepare, prepares)
}

object CommitCertificate {
  implicit val ordering: Ordering[Commit] =
    Ordering.by(commit => (commit.from, commit.localTimestamp))

  def fromProto(
      commitCertificate: ProtoCommitCertificate
  ): ParsingResult[CommitCertificate] =
    for {
      consensusPP <- ProtoConverter
        .required("prePrepare", commitCertificate.prePrepare)
      prePrepare <- PrePrepare.fromProtoConsensusMessage(consensusPP)
      commits <- commitCertificate.commits.traverse(Commit.fromProtoConsensusMessage)
    } yield CommitCertificate(prePrepare, commits)
}
