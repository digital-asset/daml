// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.bfttime

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.CanonicalCommitSet as ProtoCanonicalCommitSet
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PbftNetworkMessage,
}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

import scala.collection.immutable.SortedSet

final case class CanonicalCommitSet(private val commits: Set[Commit]) {
  lazy val sortedCommits: Seq[Commit] = SortedSet.from(commits).toSeq

  val timestamps: Seq[CantonTimestamp] = sortedCommits.map(_.localTimestamp)

  def toProto: ProtoCanonicalCommitSet = ProtoCanonicalCommitSet.of(sortedCommits.map(_.toProto))
}

object CanonicalCommitSet {
  def fromProto(canonicalCommitSet: ProtoCanonicalCommitSet): ParsingResult[CanonicalCommitSet] =
    canonicalCommitSet.canonicalCommits
      .map(consensusMessage =>
        for {
          header <- PbftNetworkMessage.headerFromProto(consensusMessage)
          protoCommit <- consensusMessage.message.commit
            .toRight(ProtoDeserializationError.OtherError("Not a commit message"))
          commit <- ConsensusSegment.ConsensusMessage.Commit
            .fromProto(
              header.blockMetadata,
              header.viewNumber,
              header.timestamp,
              protoCommit,
              header.from,
            )
        } yield commit
      )
      .sequence
      .map(x => CanonicalCommitSet(SortedSet.from(x)))
}
