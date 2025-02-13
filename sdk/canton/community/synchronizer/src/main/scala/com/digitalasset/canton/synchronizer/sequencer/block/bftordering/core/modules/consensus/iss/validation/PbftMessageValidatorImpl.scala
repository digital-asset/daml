// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare

trait PbftMessageValidator {
  def validatePrePrepare(prePrepare: PrePrepare, firstInSegment: Boolean): Either[String, Unit]
}

final class PbftMessageValidatorImpl(epoch: Epoch, metrics: BftOrderingMetrics)(
    abort: String => Nothing
)(implicit
    mc: MetricsContext
) extends PbftMessageValidator {

  def validatePrePrepare(prePrepare: PrePrepare, firstInSegment: Boolean): Either[String, Unit] = {
    val block = prePrepare.block
    val prePrepareEpochNumber = prePrepare.blockMetadata.epochNumber
    val validatorEpochNumber = epoch.info.number
    val canonicalCommitSet = prePrepare.canonicalCommitSet.sortedCommits
    val canonicalCommitSetEpochs = canonicalCommitSet.map(_.message.blockMetadata.epochNumber)

    if (validatorEpochNumber != prePrepareEpochNumber) {
      // Messages/PrePrepares from wrong/different epochs are filtered out at the Consensus module level.
      //  Getting here means a programming bug, where likely the validator is created for a wrong epoch.
      abort(
        s"Internal invariant violation: validator epoch ($validatorEpochNumber) differs from PrePrepare epoch ($prePrepareEpochNumber)"
      )
    }

    // A canonical commit set for a non-empty block should only be empty for the first block after state transfer,
    //  but it's hard to fully enforce.
    val canonicalCommitSetCanBeEmpty = block.proofs.isEmpty || firstInSegment

    // The first blocks proposed by each genesis leader should be empty so that actual transactions are not assigned
    //  inaccurate timestamps due to empty canonical commit sets.
    val blockCanBeNonEmpty = prePrepareEpochNumber != EpochNumber.First || !firstInSegment

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
        canonicalCommitSet.isEmpty || canonicalCommitSetEpochs.distinct.sizeIs == 1,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commits contain different epochs $canonicalCommitSetEpochs, should contain just one"
        },
      )

      // TODO(#23335): further validate canonical commits

      _ <- validateCanonicalCommitSetQuorum(prePrepare)

      _ <- validateProofsOfAvailability(prePrepare)

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

  private def validateProofsOfAvailability(prePrepare: PrePrepare): Either[String, Unit] =
    forallEither(prePrepare.block.proofs) { poa =>
      val howManyDistinctNodesHaveAcked = poa.acks.map(_.from).toSet.size
      val fromSequencers = poa.acks.map(_.from)
      val weakQuorum = epoch.currentMembership.orderingTopology.weakQuorum

      for {
        _ <- Either.cond(
          fromSequencers.sizeIs == fromSequencers.distinct.size,
          (), {
            emitNonComplianceMetrics(prePrepare)
            s"PrePrepare for block ${prePrepare.blockMetadata} with proof of availability have acks from same sequencer"
          },
        )
        _ <- Either.cond(
          fromSequencers.sizeIs >= weakQuorum,
          (), {
            emitNonComplianceMetrics(prePrepare)
            s"PrePrepare for block ${prePrepare.blockMetadata} with proof of availability have only $howManyDistinctNodesHaveAcked acks" +
              s" but should have at least $weakQuorum"
          },
        )
      } yield ()
    }

  private def validateCanonicalCommitSetQuorum(
      prePrepare: PrePrepare
  ) = {
    val canonicalCommitSet = prePrepare.canonicalCommitSet.sortedCommits
    val epochNumber = epoch.info.number

    canonicalCommitSet
      .map(_.message.blockMetadata.epochNumber)
      .headOption
      .map { canonicalCommitSetEpoch =>
        val previousEpochNumber = epochNumber - 1
        val messagePrefix =
          s"Canonical commit set has size ${canonicalCommitSet.size} which is below the strong quorum of"
        if (canonicalCommitSetEpoch == epochNumber) {
          val topology = epoch.currentMembership.orderingTopology
          Either.cond(
            topology.hasStrongQuorum(canonicalCommitSet.size),
            (), {
              emitNonComplianceMetrics(prePrepare)
              s"$messagePrefix ${topology.strongQuorum}"
            },
          )
        } else if (canonicalCommitSetEpoch == previousEpochNumber) {
          val topology = epoch.previousMembership.orderingTopology
          Either.cond(
            topology.hasStrongQuorum(canonicalCommitSet.size),
            (), {
              emitNonComplianceMetrics(prePrepare)
              s"$messagePrefix ${topology.strongQuorum}"
            },
          )
        } else {
          emitNonComplianceMetrics(prePrepare)
          Left(
            s"Canonical commit set epoch is $canonicalCommitSetEpoch but should be $epochNumber or $previousEpochNumber"
          )
        }
      }
      .getOrElse(Right(()))
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

  private def forallEither[X](
      xs: Seq[X]
  )(predicate: X => Either[String, Unit]): Either[String, Unit] = {
    val (errors, _) = xs.view.map(predicate).partitionMap(identity)
    errors.headOption match {
      case Some(error) => Left(error)
      case None => Right(())
    }
  }
}
