// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.Order.*
import cats.data.{Chain, EitherT}
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.CanSubmitTransfer
import com.digitalasset.canton.participant.protocol.transfer.{
  AdminPartiesAndParticipants,
  TransferOutProcessorError,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.AutomaticTransferForTransactionFailure
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
      submitters: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    // (contract id, (transfer submitter, target domain id))
    type SingleTransfer = (LfContractId, (LfPartyId, DomainId))

    val targetSnapshotET =
      EitherT.fromEither[Future](snapshotProvider.getTopologySnapshotFor(targetDomain))

    val transfers: EitherT[Future, TransactionRoutingError, Chain[SingleTransfer]] = {
      Chain.fromSeq(contracts).parFlatTraverse { c =>
        val contractDomain = c.domain

        if (contractDomain == targetDomain) EitherT.pure(Chain.empty)
        else {
          for {
            sourceSnapshot <- EitherT
              .fromEither[Future](snapshotProvider.getTopologySnapshotFor(contractDomain))
            targetSnapshot <- targetSnapshotET
            submitter <- findSubmitterThatCanTransferContract(
              sourceSnapshot,
              targetSnapshot,
              c.id,
              c.stakeholders,
              submitters,
            )
          } yield Chain(c.id -> (submitter, contractDomain))
        }
      }
    }

    transfers.map(transfers =>
      DomainRank(
        transfers.toList.toMap,
        priorityOfDomain(targetDomain),
        targetDomain,
      )
    )
  }

  private def findSubmitterThatCanTransferContract(
      sourceSnapshot: TopologySnapshot,
      targetSnapshot: TopologySnapshot,
      contractId: LfContractId,
      contractStakeholders: Set[LfPartyId],
      submitters: Set[LfPartyId],
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, LfPartyId] = {

    // Building the transfer out requests lets us check whether contract can be transferred to target domain
    def go(
        submitters: List[LfPartyId],
        errAccum: List[String] = List.empty,
    ): EitherT[Future, String, LfPartyId] = {
      submitters match {
        case Nil =>
          EitherT.leftT(show"Cannot transfer contract $contractId: ${errAccum.mkString(",")}")
        case submitter :: rest =>
          val result =
            for {
              _ <- CanSubmitTransfer.transferOut(
                contractId,
                sourceSnapshot,
                submitter,
                participantId,
              )
              adminParties <- AdminPartiesAndParticipants(
                contractId,
                submitter,
                contractStakeholders,
                sourceSnapshot,
                targetSnapshot,
                logger,
              )
            } yield adminParties
          result
            .onShutdown(Left(TransferOutProcessorError.AbortedDueToShutdownOut(contractId)))
            .biflatMap(
              left => go(rest, errAccum :+ show"Submitter $submitter cannot transfer: $left"),
              _ => EitherT.rightT(submitter),
            )
      }
    }

    go(submitters.intersect(contractStakeholders).toList).leftMap(errors =>
      AutomaticTransferForTransactionFailure.Failed(errors)
    )
  }
}

private[routing] final case class DomainRank(
    transfers: Map[LfContractId, (LfPartyId, DomainId)], // (cid, (submitter, current domain))
    priority: Int,
    domainId: DomainId, // domain for submission
)

private[routing] object DomainRank {
  // The highest priority domain should be picked first, so negate the priority
  implicit val domainRanking: Ordering[DomainRank] =
    Ordering.by(x => (-x.priority, x.transfers.size, x.domainId))
}
