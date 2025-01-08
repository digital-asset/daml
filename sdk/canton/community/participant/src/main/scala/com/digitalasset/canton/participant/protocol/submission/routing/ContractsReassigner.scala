// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.parallel.*
import com.digitalasset.canton.data.ReassignmentSubmitterMetadata
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.AutomaticReassignmentForTransactionFailure
import com.digitalasset.canton.participant.sync.{ConnectedDomainsLookup, TransactionRoutingError}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.{ExecutionContext, Future}

private[routing] class ContractsReassigner(
    connectedDomains: ConnectedDomainsLookup,
    submittingParticipant: ParticipantId,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  def reassign(
      domainRankTarget: DomainRank,
      submitterInfo: SubmitterInfo,
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, Unit] =
    if (domainRankTarget.reassignments.nonEmpty) {
      logger.info(
        s"Automatic transaction reassignment to domain ${domainRankTarget.synchronizerId}"
      )
      domainRankTarget.reassignments.toSeq.parTraverse_ {
        case (cid, (lfParty, sourceSynchronizerId)) =>
          perform(
            Source(sourceSynchronizerId),
            Target(domainRankTarget.synchronizerId),
            ReassignmentSubmitterMetadata(
              submitter = lfParty,
              submittingParticipant,
              submitterInfo.commandId,
              submitterInfo.submissionId,
              submitterInfo.applicationId,
              workflowId = None,
            ),
            cid,
          )
      }
    } else {
      EitherT.pure[Future, TransactionRoutingError](())
    }

  private def perform(
      sourceDomain: Source[SynchronizerId],
      targetDomain: Target[SynchronizerId],
      submitterMetadata: ReassignmentSubmitterMetadata,
      contractId: LfContractId,
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, Unit] = {
    val reassignment = for {
      sourceSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(sourceDomain.unwrap).toRight("Not connected to the source domain")
      )

      targetSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(targetDomain.unwrap).toRight("Not connected to the target domain")
      )

      _unit <- EitherT
        .cond[Future](sourceSyncDomain.ready, (), "The source domain is not ready for submissions")

      unassignmentResult <- sourceSyncDomain
        .submitUnassignment(
          submitterMetadata,
          contractId,
          targetDomain,
          Target(targetSyncDomain.staticSynchronizerParameters.protocolVersion),
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .leftMap(_.toString)
        .onShutdown(Left("Application is shutting down"))
      unassignmentStatus <- EitherT.right[String](unassignmentResult.unassignmentCompletionF)
      _unassignmentApprove <- EitherT.cond[Future](
        unassignmentStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The reassignment out for ${unassignmentResult.reassignmentId} failed with status $unassignmentStatus",
      )

      _unit <- EitherT
        .cond[Future](targetSyncDomain.ready, (), "The target domain is not ready for submission")

      assignmentResult <- targetSyncDomain
        .submitAssignment(
          submitterMetadata,
          unassignmentResult.reassignmentId,
        )
        .leftMap[String](err => s"Assignment failed with error $err")
        .flatMap { s =>
          EitherT(s.map(Right(_)).onShutdown(Left("Application is shutting down")))
        }

      assignmentStatus <- EitherT.right[String](assignmentResult.assignmentCompletionF)
      _assignmentApprove <- EitherT.cond[Future](
        assignmentStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The assignment for ${unassignmentResult.reassignmentId} failed with verdict $assignmentStatus",
      )
    } yield ()

    reassignment.leftMap[TransactionRoutingError](str =>
      AutomaticReassignmentForTransactionFailure.Failed(str)
    )
  }
}
