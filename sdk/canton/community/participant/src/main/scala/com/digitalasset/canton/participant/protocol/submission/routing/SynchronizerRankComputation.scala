// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.{Chain, EitherT}
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.ReassignmentRef
import com.digitalasset.canton.ledger.participant.state.SynchronizerRank
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment.{
  ReassigningParticipantsComputation,
  ReassignmentValidation,
  ReassignmentValidationError,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.AutomaticReassignmentForTransactionFailure
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.{ExecutionContext, Future}

private[routing] class SynchronizerRankComputation(
    participantId: ParticipantId,
    priorityOfSynchronizer: SynchronizerId => Int,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  import com.digitalasset.canton.util.ShowUtil.*

  def computeBestSynchronizerRank(
      synchronizerState: RoutingSynchronizerState,
      contracts: Seq[ContractData],
      readers: Set[LfPartyId],
      synchronizerIds: NonEmpty[Set[SynchronizerId]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] =
    EitherT {
      for {
        rankedSynchronizers <- synchronizerIds.forgetNE.toList
          .parTraverseFilter(targetSynchronizer =>
            compute(
              contracts,
              Target(targetSynchronizer),
              readers,
              synchronizerState,
            )
              // TODO(#23334): The resulting error is discarded in toOption. Consider forwarding it instead
              .toOption.value
          )
        // Priority of synchronizer
        // Number of reassignments if we use this synchronizer
        // pick according to the least amount of reassignments
      } yield rankedSynchronizers.minOption
        .toRight(
          // TODO(#23334): Revisit this reported error as it can be misleading
          TransactionRoutingError.AutomaticReassignmentForTransactionFailure.Failed(
            s"None of the following $synchronizerIds is suitable for automatic reassignment."
          )
        )
    }

  // Includes check that submitting party has a participant with submission rights on source and target synchronizer
  def compute(
      contracts: Seq[ContractData],
      targetSynchronizer: Target[SynchronizerId],
      readers: Set[LfPartyId],
      synchronizerState: RoutingSynchronizerState,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, SynchronizerRank] = {
    // (contract id, (reassignment submitter, target synchronizer id))
    type SingleReassignment = (LfContractId, (LfPartyId, SynchronizerId))

    val targetSnapshotET =
      EitherT.fromEither[FutureUnlessShutdown](
        synchronizerState.getTopologySnapshotFor(targetSynchronizer)
      )

    val reassignmentsET
        : EitherT[FutureUnlessShutdown, TransactionRoutingError, Chain[SingleReassignment]] =
      Chain.fromSeq(contracts).parFlatTraverse { c =>
        val contractAssignation = c.synchronizerId

        if (contractAssignation == targetSynchronizer.unwrap) EitherT.pure(Chain.empty)
        else {
          for {
            sourceSnapshot <- EitherT
              .fromEither[FutureUnlessShutdown](
                synchronizerState.getTopologySnapshotFor(contractAssignation)
              )
              .map(Source(_))
            targetSnapshot <- targetSnapshotET
            submitter <- findReaderThatCanReassignContract(
              sourceSnapshot = sourceSnapshot,
              sourceSynchronizerId = Source(contractAssignation),
              targetSnapshot = targetSnapshot,
              targetSynchronizerId = targetSynchronizer,
              contract = c,
              readers = readers,
            ).mapK(FutureUnlessShutdown.outcomeK)
          } yield Chain(c.id -> (submitter, contractAssignation))
        }
      }

    reassignmentsET.map(reassignments =>
      SynchronizerRank(
        reassignments.toList.toMap,
        priorityOfSynchronizer(targetSynchronizer.unwrap),
        targetSynchronizer.unwrap,
      )
    )
  }

  private def findReaderThatCanReassignContract(
      sourceSnapshot: Source[TopologySnapshot],
      sourceSynchronizerId: Source[SynchronizerId],
      targetSnapshot: Target[TopologySnapshot],
      targetSynchronizerId: Target[SynchronizerId],
      contract: ContractData,
      readers: Set[LfPartyId],
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, LfPartyId] = {
    logger.debug(
      s"Computing submitter that can submit reassignment of ${contract.id} with stakeholders ${contract.stakeholders} from $sourceSynchronizerId to $targetSynchronizerId. Candidates are: $readers"
    )

    // Building the unassignment requests lets us check whether contract can be reassigned to target synchronizer
    def go(
        readers: List[LfPartyId],
        errAccum: List[String] = List.empty,
    ): EitherT[Future, String, LfPartyId] =
      readers match {
        case Nil =>
          EitherT.leftT(
            show"Cannot reassign contract ${contract.id} from $sourceSynchronizerId to $targetSynchronizerId: ${errAccum
                .mkString(",")}"
          )
        case reader :: rest =>
          val result =
            for {
              _ <- ReassignmentValidation
                .checkSubmitter(
                  ReassignmentRef(contract.id),
                  sourceSnapshot,
                  reader,
                  participantId,
                  contract.stakeholders.all,
                )
              _ <- new ReassigningParticipantsComputation(
                stakeholders = contract.stakeholders,
                sourceSnapshot,
                targetSnapshot,
              ).compute.leftWiden[ReassignmentValidationError]
            } yield ()
          result
            .onShutdown(Left(ReassignmentValidationError.AbortedDueToShutdownOut(contract.id)))
            .biflatMap(
              left => go(rest, errAccum :+ show"Read $reader cannot reassign: $left"),
              _ => EitherT.rightT(reader),
            )
      }

    go(readers.intersect(contract.stakeholders.all).toList).leftMap(errors =>
      AutomaticReassignmentForTransactionFailure.Failed(errors)
    )
  }
}
