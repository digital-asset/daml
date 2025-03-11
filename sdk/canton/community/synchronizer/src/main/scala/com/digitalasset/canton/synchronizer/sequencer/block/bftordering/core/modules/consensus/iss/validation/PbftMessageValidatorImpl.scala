// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.syntax.traverse.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare

trait PbftMessageValidator {
  def validatePrePrepare(prePrepare: PrePrepare): Either[String, Unit]
}

final class PbftMessageValidatorImpl(segment: Segment, epoch: Epoch, metrics: BftOrderingMetrics)(
    abort: String => Nothing
)(implicit mc: MetricsContext, config: BftBlockOrdererConfig)
    extends PbftMessageValidator {

  import PbftMessageValidatorImpl.*

  def validatePrePrepare(prePrepare: PrePrepare): Either[String, Unit] = {
    val block = prePrepare.block
    val blockFirstInSegment = segment.isFirstInSegment(prePrepare.blockMetadata.blockNumber)
    val prePrepareEpochNumber = prePrepare.blockMetadata.epochNumber
    val validatorEpochNumber = epoch.info.number

    if (validatorEpochNumber != prePrepareEpochNumber) {
      // Messages/PrePrepares from wrong/different epochs are filtered out at the Consensus module level.
      //  Getting here means a programming bug, where likely the validator is created for a wrong epoch.
      abort(
        s"Internal invariant violation: validator epoch ($validatorEpochNumber) differs from PrePrepare epoch ($prePrepareEpochNumber)"
      )
    }

    // The first blocks proposed by each genesis leader should be empty so that actual transactions are not assigned
    //  inaccurate timestamps due to empty canonical commit sets.
    val blockCanBeNonEmpty = prePrepareEpochNumber != EpochNumber.First || !blockFirstInSegment

    for {
      _ <- Either.cond(
        block.proofs.sizeIs <= config.maxBatchesPerBlockProposal.toInt,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"PrePrepare for block ${prePrepare.blockMetadata} has ${block.proofs.size} proofs of availability, " +
            s"but it should have up to the maximum batch number per proposal of ${config.maxBatchesPerBlockProposal}, " +
            "this number should be configured to the same value across all nodes"
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

      _ <- validateProofsOfAvailability(prePrepare)

      _ <- validateCanonicalCommitSet(prePrepare, blockFirstInSegment)
    } yield ()
  }

  private def validateProofsOfAvailability(prePrepare: PrePrepare): Either[String, Unit] =
    forallEither(prePrepare.block.proofs) { poa =>
      val howManyDistinctNodesHaveAcked = poa.acks.map(_.from).toSet.size
      val fromSequencers = poa.acks.map(_.from)
      val weakQuorum = epoch.currentMembership.orderingTopology.weakQuorum

      for {
        _ <- Either.cond(
          poa.expirationTime > epoch.info.previousEpochMaxBftTime,
          (), {
            emitNonComplianceMetrics(prePrepare)
            s"PrePrepare for block ${prePrepare.blockMetadata} has expired proof of availability at ${poa.expirationTime}, (current time is ${epoch.info.previousEpochMaxBftTime})"
          },
        )
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
            s"PrePrepare for block ${prePrepare.blockMetadata} with proof of availability have only " +
              s"$howManyDistinctNodesHaveAcked acks but should have at least $weakQuorum"
          },
        )
      } yield ()
    }

  private def validateCanonicalCommitSet(prePrepare: PrePrepare, firstInSegment: Boolean) = {
    val block = prePrepare.block
    val prePrepareEpochNumber = prePrepare.blockMetadata.epochNumber
    val prePrepareBlockNumber = prePrepare.blockMetadata.blockNumber
    val canonicalCommitSet = prePrepare.canonicalCommitSet.sortedCommits
    val canonicalCommitSetEpochNumbers = canonicalCommitSet.map(_.message.blockMetadata.epochNumber)
    val canonicalCommitSetBlockNumbers = canonicalCommitSet.map(_.message.blockMetadata.blockNumber)
    // A canonical commit set for a non-empty block should only be empty for the first block after state transfer,
    //  but it's hard to fully enforce.
    val canonicalCommitSetCanBeEmpty = block.proofs.isEmpty || firstInSegment

    for {
      _ <- Either.cond(
        canonicalCommitSet.nonEmpty || canonicalCommitSetCanBeEmpty,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commit set is empty for block ${prePrepare.blockMetadata} with ${block.proofs.size} " +
            "proofs of availability, but it can only be empty for empty blocks or first blocks in segments"
        },
      )

      senderIds = canonicalCommitSet.map(_.from)
      _ <- Either.cond(
        senderIds.distinct.sizeIs == canonicalCommitSet.size,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commits for block ${prePrepare.blockMetadata} contain duplicate senders: $senderIds"
        },
      )

      distinctEpochNumbers = canonicalCommitSetEpochNumbers.distinct
      _ <- Either.cond(
        canonicalCommitSet.isEmpty || distinctEpochNumbers.sizeIs == 1,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commits contain different epochs $distinctEpochNumbers, should contain just one"
        },
      )

      distinctBlockNumbers = canonicalCommitSetBlockNumbers.distinct
      _ <- Either.cond(
        canonicalCommitSet.isEmpty || distinctBlockNumbers.sizeIs == 1,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commits contain different block numbers $distinctBlockNumbers, should contain just one"
        },
      )

      canonicalCommitSetFirstEpochNumber = canonicalCommitSetEpochNumbers.headOption
      _ <- Either.cond(
        canonicalCommitSetFirstEpochNumber.forall(epochNumber =>
          firstInSegment || epochNumber == prePrepareEpochNumber
        ),
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commits contain epoch number $canonicalCommitSetFirstEpochNumber that is different from " +
            s"PrePrepare's epoch number $prePrepareEpochNumber"
        },
      )

      previousEpochNumber = prePrepareEpochNumber - 1
      _ <- Either.cond(
        canonicalCommitSetFirstEpochNumber.forall(epochNumber =>
          !firstInSegment || epochNumber == previousEpochNumber
        ),
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commits for the first block in the segment contain epoch number $canonicalCommitSetFirstEpochNumber " +
            s"that is different from PrePrepare's previous epoch number $previousEpochNumber"
        },
      )

      canonicalCommitSetFirstBlockNumber = canonicalCommitSetBlockNumbers.headOption
      lastBlockFromPreviousEpoch = epoch.info.startBlockNumber - 1
      _ <- Either.cond(
        canonicalCommitSetFirstBlockNumber.forall(blockNumber =>
          !firstInSegment || blockNumber == lastBlockFromPreviousEpoch
        ),
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commits for the first block in the segment refer to block number $canonicalCommitSetFirstBlockNumber " +
            s"that is not the last block from the previous epoch ($lastBlockFromPreviousEpoch)"
        },
      )

      previousBlockNumberInSegment = segment.previousBlockNumberInSegment(prePrepareBlockNumber)
      _ <- Either.cond(
        canonicalCommitSetFirstBlockNumber.forall(blockNumber =>
          firstInSegment || previousBlockNumberInSegment.contains(blockNumber)
        ),
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"Canonical commits contain block number $canonicalCommitSetFirstBlockNumber that does not refer to " +
            s"the previous block ($previousBlockNumberInSegment) in the current segment ${segment.slotNumbers}"
        },
      )

      _ <- validateCanonicalCommitSetQuorum(prePrepare)
    } yield ()
  }

  private def validateCanonicalCommitSetQuorum(
      prePrepare: PrePrepare
  ) = {
    val canonicalCommitSet = prePrepare.canonicalCommitSet.sortedCommits
    val epochNumber = epoch.info.number

    def validate(topology: OrderingTopology) =
      for {
        _ <- Either.cond(
          topology.hasStrongQuorum(canonicalCommitSet.size),
          (), {
            emitNonComplianceMetrics(prePrepare)
            s"Canonical commit set for block ${prePrepare.blockMetadata} has size ${canonicalCommitSet.size} " +
              s"which is below the strong quorum of ${topology.strongQuorum}"
          },
        )
        _ <- canonicalCommitSet.traverse(commit =>
          Either.cond(
            topology.contains(commit.from),
            (), {
              emitNonComplianceMetrics(prePrepare)
              s"Canonical commit set for block ${prePrepare.blockMetadata} contains commit from ${commit.from} " +
                s"that is not part of current topology ${topology.peers}"
            },
          )
        )
      } yield ()

    canonicalCommitSet
      .map(_.message.blockMetadata.epochNumber)
      .headOption
      .map { canonicalCommitSetEpoch =>
        val previousEpochNumber = epochNumber - 1

        if (canonicalCommitSetEpoch == epochNumber) {
          val topology = epoch.currentMembership.orderingTopology
          validate(topology)
        } else if (canonicalCommitSetEpoch == previousEpochNumber) {
          val topology = epoch.previousMembership.orderingTopology
          validate(topology)
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
}

object PbftMessageValidatorImpl {

  private def forallEither[X](
      xs: Seq[X]
  )(predicate: X => Either[String, Unit]): Either[String, Unit] =
    xs.traverse(predicate).map(_ => ())
}
