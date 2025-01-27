// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare

trait PbftMessageValidator {
  def validatePrePrepare(prePrepare: PrePrepare, firstInSegment: Boolean): Either[String, Unit]
}

final class PbftMessageValidatorImpl(metrics: BftOrderingMetrics)(implicit mc: MetricsContext)
    extends PbftMessageValidator {

  def validatePrePrepare(prePrepare: PrePrepare, firstInSegment: Boolean): Either[String, Unit] = {
    // TODO(#17108): verify PrePrepare is sound in terms of ProofsOfAvailability
    // TODO(#23335): further validate canonical commits
    val block = prePrepare.block
    val canonicalCommitSet = prePrepare.canonicalCommitSet.sortedCommits

    // A canonical commit set for a non-empty block should only be empty for the first block after state transfer,
    //  but it's hard to fully enforce.
    val canonicalCommitSetCanBeEmpty = block.proofs.isEmpty || firstInSegment

    // The first blocks proposed by each genesis leader should be empty so that actual transactions are not assigned
    //  inaccurate timestamps due to empty canonical commit sets.
    val blockCanBeNonEmpty =
      prePrepare.blockMetadata.epochNumber != EpochNumber.First || !firstInSegment

    for {
      _ <- Either.cond(
        canonicalCommitSet.nonEmpty || canonicalCommitSetCanBeEmpty,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commit set is empty for block ${prePrepare.blockMetadata} with ${block.proofs.size} " +
            "proofs of availability, but it can only be empty for empty blocks or first blocks in segments"
        },
      )

      _ <- Either.cond(
        blockCanBeNonEmpty || block.proofs.isEmpty,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"PrePrepare for block ${prePrepare.blockMetadata} has ${block.proofs.size} proofs of availability, " +
            s"but it should be empty"
        },
      )
    } yield ()
  }

  private def emitNonComplianceMetrics(prePrepare: PrePrepare): Unit = {
    val blockMetadata = prePrepare.blockMetadata
    emitNonCompliance(metrics)(
      prePrepare.from,
      blockMetadata.epochNumber,
      prePrepare.viewNumber,
      blockMetadata.blockNumber,
      metrics.security.noncompliant.labels.violationType.values.ConsensusInvalidMessage,
    )
  }
}
