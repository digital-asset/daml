// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.Order.*
import cats.data.{Chain, EitherT}
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.ReassignmentRef
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
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.{ExecutionContext, Future}

private[routing] class DomainRankComputation(
    participantId: ParticipantId,
    priorityOfSynchronizer: SynchronizerId => Int,
    snapshotProvider: DomainStateProvider,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  import com.digitalasset.canton.util.ShowUtil.*

  // Includes check that submitting party has a participant with submission rights on source and target domain
  def compute(
      contracts: Seq[ContractData],
      targetDomain: Target[SynchronizerId],
      readers: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    // (contract id, (reassignment submitter, target synchronizer id))
    type SingleReassignment = (LfContractId, (LfPartyId, SynchronizerId))

    val targetSnapshotET =
      EitherT.fromEither[Future](snapshotProvider.getTopologySnapshotFor(targetDomain))

    val reassignmentsET: EitherT[Future, TransactionRoutingError, Chain[SingleReassignment]] =
      Chain.fromSeq(contracts).parFlatTraverse { c =>
        val contractDomain = c.synchronizerId

        if (contractDomain == targetDomain.unwrap) EitherT.pure(Chain.empty)
        else {
          for {
            sourceSnapshot <- EitherT
              .fromEither[Future](snapshotProvider.getTopologySnapshotFor(contractDomain))
              .map(Source(_))
            targetSnapshot <- targetSnapshotET
            submitter <- findReaderThatCanReassignContract(
              sourceSnapshot = sourceSnapshot,
              sourceSynchronizerId = Source(contractDomain),
              targetSnapshot = targetSnapshot,
              targetSynchronizerId = targetDomain,
              contract = c,
              readers = readers,
            )
          } yield Chain(c.id -> (submitter, contractDomain))
        }
      }

    reassignmentsET.map(reassignments =>
      DomainRank(
        reassignments.toList.toMap,
        priorityOfSynchronizer(targetDomain.unwrap),
        targetDomain.unwrap,
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

    // Building the unassignment requests lets us check whether contract can be reassigned to target domain
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

private[routing] final case class DomainRank(
    reassignments: Map[
      LfContractId,
      (LfPartyId, SynchronizerId),
    ], // (cid, (submitter, current domain))
    priority: Int,
    synchronizerId: SynchronizerId, // domain for submission
)

private[routing] object DomainRank {
  // The highest priority domain should be picked first, so negate the priority
  implicit val domainRanking: Ordering[DomainRank] =
    Ordering.by(x => (-x.priority, x.reassignments.size, x.synchronizerId))
}
