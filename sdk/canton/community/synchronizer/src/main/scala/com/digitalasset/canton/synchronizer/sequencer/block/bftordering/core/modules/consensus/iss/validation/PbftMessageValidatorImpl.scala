// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.syntax.traverse.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.{
  BftBlockOrdererConfig,
  FingerprintKeyId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.{
  Epoch,
  Segment,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.AvailabilityAck.ValidationError
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.ProofOfAvailability
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
        s"Internal invariant violation: validator epoch ($validatorEpochNumber) " +
          s"differs from PrePrepare epoch ($prePrepareEpochNumber)"
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
          s"The PrePrepare for block ${prePrepare.blockMetadata} has ${block.proofs.size} proofs of availability, " +
            s"but it should have up to the maximum batch number per proposal of ${config.maxBatchesPerBlockProposal}; " +
            "the maximum batch number per proposal should be configured with the same value across all nodes"
        },
      )

      _ <- Either.cond(
        blockCanBeNonEmpty || block.proofs.isEmpty,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"The PrePrepare for block ${prePrepare.blockMetadata} has ${block.proofs.size} proofs of availability, " +
            "but it should be empty"
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
        _ <- validateProofOfAvailabilityAgainstCurrentTopology(poa, prePrepare)

        _ <- Either.cond(
          poa.epochNumber <= epoch.info.number,
          (), {
            emitNonComplianceMetrics(prePrepare)
            s"The PrePrepare for block ${prePrepare.blockMetadata} has a proof of availability for future epoch " +
              s"${poa.epochNumber} (current epoch is ${epoch.info.number})"
          },
        )
        _ <- Either.cond(
          poa.epochNumber > epoch.info.number - OrderingRequestBatch.BatchValidityDurationEpochs,
          (), {
            emitNonComplianceMetrics(prePrepare)
            s"The PrePrepare for block ${prePrepare.blockMetadata} has an expired proof of availability " +
              s"at ${poa.epochNumber}, which is ${OrderingRequestBatch.BatchValidityDurationEpochs} " +
              s"epochs or more older than current epoch ${epoch.info.number}."
          },
        )
        _ <- Either.cond(
          fromSequencers.sizeIs == fromSequencers.distinct.size,
          (), {
            emitNonComplianceMetrics(prePrepare)
            s"The proof of availability for batch ${poa.batchId} in PrePrepare for block ${prePrepare.blockMetadata} " +
              "has duplicated dissemination acknowledgements"
          },
        )
        _ <- Either.cond(
          fromSequencers.sizeIs >= weakQuorum,
          (), {
            emitNonComplianceMetrics(prePrepare)
            s"The proof of availability for batch ${poa.batchId} in PrePrepare for block ${prePrepare.blockMetadata} " +
              s"has $howManyDistinctNodesHaveAcked dissemination acknowledgements, " +
              s"but it should have at least $weakQuorum"
          },
        )
      } yield ()
    }

  private def validateProofOfAvailabilityAgainstCurrentTopology(
      poa: ProofOfAvailability,
      prePrepare: PrePrepare,
  ): Either[String, Unit] =
    forallEither(poa.acks) { ack =>
      val ackValidationResult = ack.validateIn(epoch.currentMembership.orderingTopology)

      ackValidationResult match {

        case Left(error) =>
          emitNonComplianceMetrics(prePrepare)

          val errorString =
            error match {

              case ValidationError.NodeNotInTopology =>
                s"The dissemination acknowledgement for batch ${poa.batchId} from '${ack.from}' is invalid " +
                  s"because '${ack.from}' is not in the current topology " +
                  s"(epoch ${epoch.info.number}, nodes ${epoch.currentMembership.orderingTopology.nodes})"

              case ValidationError.KeyNotInTopology =>
                val nodeTopologyInfo =
                  epoch.currentMembership.orderingTopology.nodesTopologyInfo
                    .getOrElse(
                      ack.from,
                      abort(
                        s"Internal invariant violation: node '${ack.from}' is part of the current topology" +
                          s"(epoch ${epoch.info.number}), but has no associated topology info"
                      ),
                    )
                val keyId = FingerprintKeyId.toBftKeyId(ack.signature.signedBy)
                s"The dissemination acknowledgement for batch ${poa.batchId} from '${ack.from}' is invalid " +
                  s"because the signing key '$keyId' is not valid for '${ack.from}' in the current topology " +
                  s"(epoch ${epoch.info.number}, nodes ${epoch.currentMembership.orderingTopology.nodes}); " +
                  s"the keys valid for '${ack.from}' in the current topology are ${nodeTopologyInfo.keyIds}"
            }

          Left(errorString)

        case Right(_) => Right(())
      }
    }

  private def validateCanonicalCommitSet(
      prePrepare: PrePrepare,
      firstInSegment: Boolean,
  ): Either[String, Unit] = {
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
          s"The canonical commit set is empty for block ${prePrepare.blockMetadata} with ${block.proofs.size} " +
            "proofs of availability, but it can only be empty for empty blocks or the first segment block"
        },
      )

      senderIds = canonicalCommitSet.map(_.from)
      _ <- Either.cond(
        senderIds.distinct.sizeIs == canonicalCommitSet.size,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"The canonical commit set for block ${prePrepare.blockMetadata} contains duplicate senders: $senderIds"
        },
      )

      distinctEpochNumbers = canonicalCommitSetEpochNumbers.distinct
      _ <- Either.cond(
        canonicalCommitSet.isEmpty || distinctEpochNumbers.sizeIs == 1,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"The canonical commit set refers to multiple epochs $distinctEpochNumbers, " +
            "but it should refer to just one"
        },
      )

      distinctBlockNumbers = canonicalCommitSetBlockNumbers.distinct
      _ <- Either.cond(
        canonicalCommitSet.isEmpty || distinctBlockNumbers.sizeIs == 1,
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"The canonical commit set refers to multiple block numbers $distinctBlockNumbers, " +
            "but it should refer to just one"
        },
      )

      canonicalCommitSetFirstEpochNumber = canonicalCommitSetEpochNumbers.headOption
      _ <- Either.cond(
        canonicalCommitSetFirstEpochNumber.forall(epochNumber =>
          firstInSegment || epochNumber == prePrepareEpochNumber
        ),
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"The canonical commit set refers to epoch number $canonicalCommitSetFirstEpochNumber " +
            s"that is different from PrePrepare's epoch number $prePrepareEpochNumber"
        },
      )

      previousEpochNumber = prePrepareEpochNumber - 1
      _ <- Either.cond(
        canonicalCommitSetFirstEpochNumber.forall(epochNumber =>
          !firstInSegment || epochNumber == previousEpochNumber
        ),
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"The canonical commit set for the first block in the segment " +
            s"refers to epoch number $canonicalCommitSetFirstEpochNumber " +
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
          s"The canonical commit set for the first block in the segment " +
            s"refers to block number $canonicalCommitSetFirstBlockNumber " +
            s"that is not the last block number from the previous epoch ($lastBlockFromPreviousEpoch)"
        },
      )

      previousBlockNumberInSegment = segment.previousBlockNumberInSegment(prePrepareBlockNumber)
      _ <- Either.cond(
        canonicalCommitSetFirstBlockNumber.forall(blockNumber =>
          firstInSegment || previousBlockNumberInSegment.contains(blockNumber)
        ),
        (), {
          emitNonComplianceMetrics(prePrepare)
          s"The canonical commit set refers to block number $canonicalCommitSetFirstBlockNumber that is not " +
            s"the previous block number $previousBlockNumberInSegment in the current segment ${segment.slotNumbers}"
        },
      )

      _ <- validateCanonicalCommitSetQuorum(prePrepare)
    } yield ()
  }

  private def validateCanonicalCommitSetQuorum(prePrepare: PrePrepare): Either[String, Unit] = {
    val canonicalCommitSet = prePrepare.canonicalCommitSet.sortedCommits
    val epochNumber = epoch.info.number

    def validate(topology: OrderingTopology) =
      for {
        _ <- Either.cond(
          topology.hasStrongQuorum(canonicalCommitSet.size),
          (), {
            emitNonComplianceMetrics(prePrepare)
            s"The canonical commit set for block ${prePrepare.blockMetadata} has size ${canonicalCommitSet.size} " +
              s"which is below the strong quorum of ${topology.strongQuorum} for current topology ${topology.nodes}"
          },
        )
        // This check is stricter than needed, i.e., all commit messages are checked. It's because nodes always send
        //  exactly the strong quorum of commits (for performance and simplicity).
        _ <- canonicalCommitSet.traverse(commit =>
          Either.cond(
            topology.contains(commit.from),
            (), {
              emitNonComplianceMetrics(prePrepare)
              s"The canonical commit set for block ${prePrepare.blockMetadata} " +
                s"contains a commit from '${commit.from}' " +
                s"that is not part of current topology ${topology.nodes}"
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
            s"The canonical commit set epoch is $canonicalCommitSetEpoch, " +
              s"but it should be either $epochNumber or $previousEpochNumber"
          )
        }
      }
      .getOrElse(Right(()))
  }

  private def emitNonComplianceMetrics(prePrepare: PrePrepare): Unit = {
    val blockMetadata = prePrepare.blockMetadata
    emitNonCompliance(metrics)(
      prePrepare.from,
      Some(blockMetadata.epochNumber),
      Some(prePrepare.viewNumber),
      Some(blockMetadata.blockNumber),
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
