// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.bfttime

import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit

import scala.collection.immutable.SortedSet

/** Leaders include, with their proposal of a new block, a strong quorum of signed commit messages that completed
  * the previous block. This is what makes this set canonical and ensures every validator agrees on it, even if it is
  * different from the commits a validator has seen, which caused the validator to commit the respective block.
  * Check the CometBFT documentation for the original concept:
  * https://docs.cometbft.com/v0.37/spec/consensus/consensus#canonical-vs-subjective-commit.
  *
  * A correct node is required to include a non-empty canonical commit set except for:
  * - each leader's first block in the first epoch
  * - bottom blocks created due to a segment view change
  * - a state-transferred node’s first block in the epoch at which it starts participating as a leader
  * However, it's non-trivial to fully enforce it, so only best-effort validation has been introduced (see
  * [[com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.validation.PbftMessageValidatorImpl.validatePrePrepare]]).
  */
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
