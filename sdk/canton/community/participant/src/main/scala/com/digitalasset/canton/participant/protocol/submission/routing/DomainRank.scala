// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.Order.*
import cats.data.{Chain, EitherT}
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.CanSubmitReassignment
import com.digitalasset.canton.participant.protocol.reassignment.{
  AdminPartiesAndParticipants,
  UnassignmentProcessorError,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.AutomaticReassignmentForTransactionFailure
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*

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
      targetDomain: DomainId,
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

        if (contractDomain == targetDomain) EitherT.pure(Chain.empty)
        else {
          for {
            sourceSnapshot <- EitherT
              .fromEither[Future](snapshotProvider.getTopologySnapshotFor(contractDomain))
            targetSnapshot <- targetSnapshotET
            submitter <- findReaderThatCanReassignContract(
              sourceSnapshot = sourceSnapshot,
              sourceDomainId = SourceDomainId(contractDomain),
              targetSnapshot = targetSnapshot,
              targetDomainId = TargetDomainId(targetDomain),
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
        priorityOfDomain(targetDomain),
        targetDomain,
      )
    )
  }

  private def findReaderThatCanReassignContract(
      sourceSnapshot: TopologySnapshot,
      sourceDomainId: SourceDomainId,
      targetSnapshot: TopologySnapshot,
      targetDomainId: TargetDomainId,
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
              _ <- CanSubmitReassignment.unassignment(
                contractId,
                sourceSnapshot,
                reader,
                participantId,
              )
              _adminParties <- AdminPartiesAndParticipants(
                contractStakeholders,
                sourceSnapshot,
                targetSnapshot,
              )
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
