// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.bfttime

import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

import scala.collection.immutable.SortedSet

final case class CanonicalCommitSet(private val commits: Set[SignedMessage[Commit]]) {
  lazy val sortedCommits: Seq[SignedMessage[Commit]] = SortedSet.from(commits).toSeq

  val timestamps: Seq[CantonTimestamp] = sortedCommits.map(_.message.localTimestamp)

  def toProto: v1.CanonicalCommitSet = v1.CanonicalCommitSet.of(sortedCommits.map(_.toProto))
}

object CanonicalCommitSet {

  val empty: CanonicalCommitSet = CanonicalCommitSet(Set.empty)

  def fromProto(canonicalCommitSet: v1.CanonicalCommitSet): ParsingResult[CanonicalCommitSet] =
    canonicalCommitSet.canonicalCommits
      .traverse(
        SignedMessage.fromProto(v1.ConsensusMessage)(Commit.fromProtoConsensusMessage)
      )
      .map(x => CanonicalCommitSet(SortedSet.from(x)))
}
