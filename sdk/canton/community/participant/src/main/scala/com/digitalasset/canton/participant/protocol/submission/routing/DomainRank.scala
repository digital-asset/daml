// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.Order.*
import cats.data.{Chain, EitherT}
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.ReassignmentSubmissionValidation
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.ReassignmentProcessorError
import com.digitalasset.canton.participant.protocol.reassignment.{
  ReassigningParticipants,
  UnassignmentProcessorError,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.AutomaticReassignmentForTransactionFailure
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.{ExecutionContext, Future}

private[routing] class DomainRankComputation(
    participantId: ParticipantId,
    priorityOfDomain: DomainId => Int,
    snapshotProvider: DomainStateProvider,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  import com.digitalasset.canton.util.ShowUtil.*

  // Includes check that submitting party has a participant with submission rights on source and target domain
  def compute(
      contracts: Seq[ContractData],
      targetDomain: Target[DomainId],
      readers: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    // (contract id, (reassignment submitter, target domain id))
    type SingleReassignment = (LfContractId, (LfPartyId, DomainId))

    val targetSnapshotET =
      EitherT.fromEither[Future](snapshotProvider.getTopologySnapshotFor(targetDomain))

    val reassignmentsET: EitherT[Future, TransactionRoutingError, Chain[SingleReassignment]] =
      Chain.fromSeq(contracts).parFlatTraverse { c =>
        val contractDomain = c.domain

        if (contractDomain == targetDomain.unwrap) EitherT.pure(Chain.empty)
        else {
          for {
            sourceSnapshot <- EitherT
              .fromEither[Future](snapshotProvider.getTopologySnapshotFor(contractDomain))
              .map(Source(_))
            targetSnapshot <- targetSnapshotET
            submitter <- findReaderThatCanReassignContract(
              sourceSnapshot = sourceSnapshot,
              sourceDomainId = Source(contractDomain),
              targetSnapshot = targetSnapshot,
              targetDomainId = targetDomain,
              contractId = c.id,
              contractStakeholders = c.stakeholders,
              readers = readers,
            )
          } yield Chain(c.id -> (submitter, contractDomain))
        }
      }

    reassignmentsET.map(reassignments =>
      DomainRank(
        reassignments.toList.toMap,
        priorityOfDomain(targetDomain.unwrap),
        targetDomain.unwrap,
      )
    )
  }

  private def findReaderThatCanReassignContract(
      sourceSnapshot: Source[TopologySnapshot],
      sourceDomainId: Source[DomainId],
      targetSnapshot: Target[TopologySnapshot],
      targetDomainId: Target[DomainId],
      contractId: LfContractId,
      contractStakeholders: Set[LfPartyId],
      readers: Set[LfPartyId],
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, LfPartyId] = {
    logger.debug(
      s"Computing submitter that can submit reassignment of $contractId with stakeholders $contractStakeholders from $sourceDomainId to $targetDomainId. Candidates are: $readers"
    )

    // Building the unassignment requests lets us check whether contract can be reassigned to target domain
    def go(
        readers: List[LfPartyId],
        errAccum: List[String] = List.empty,
    ): EitherT[Future, String, LfPartyId] =
      readers match {
        case Nil =>
          EitherT.leftT(
            show"Cannot reassign contract $contractId from $sourceDomainId to $targetDomainId: ${errAccum
                .mkString(",")}"
          )
        case reader :: rest =>
          val result =
            for {
              _ <- ReassignmentSubmissionValidation.unassignment(
                contractId,
                sourceSnapshot,
                reader,
                participantId,
                contractStakeholders,
              )
              _ <- new ReassigningParticipants(
                contractStakeholders,
                sourceSnapshot,
                targetSnapshot,
              ).compute.mapK(FutureUnlessShutdown.outcomeK).leftWiden[ReassignmentProcessorError]
            } yield ()
          result
            .onShutdown(Left(UnassignmentProcessorError.AbortedDueToShutdownOut(contractId)))
            .biflatMap(
              left => go(rest, errAccum :+ show"Read $reader cannot reassign: $left"),
              _ => EitherT.rightT(reader),
            )
      }

    go(readers.intersect(contractStakeholders).toList).leftMap(errors =>
      AutomaticReassignmentForTransactionFailure.Failed(errors)
    )
  }
}

private[routing] final case class DomainRank(
    reassignments: Map[LfContractId, (LfPartyId, DomainId)], // (cid, (submitter, current domain))
    priority: Int,
    domainId: DomainId, // domain for submission
)

private[routing] object DomainRank {
  // The highest priority domain should be picked first, so negate the priority
  implicit val domainRanking: Ordering[DomainRank] =
    Ordering.by(x => (-x.priority, x.reassignments.size, x.domainId))
}
