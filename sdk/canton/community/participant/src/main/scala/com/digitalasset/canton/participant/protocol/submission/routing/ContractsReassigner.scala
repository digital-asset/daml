// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.ReassignmentSubmitterMetadata
import com.digitalasset.canton.error.TransactionRoutingError
import com.digitalasset.canton.error.TransactionRoutingError.AutomaticReassignmentForTransactionFailure
import com.digitalasset.canton.ledger.participant.state.{SubmitterInfo, SynchronizerRank}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizersLookup
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}

import scala.concurrent.{ExecutionContext, Future}

private[routing] class ContractsReassigner(
    connectedSynchronizers: ConnectedSynchronizersLookup,
    submittingParticipant: ParticipantId,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {
  def reassign(
      synchronizerRankTarget: SynchronizerRank,
      submitterInfo: SubmitterInfo,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Unit] =
    if (synchronizerRankTarget.reassignments.nonEmpty) {
      logger.info(
        s"Automatic transaction reassignment to synchronizer ${synchronizerRankTarget.synchronizerId}"
      )

      def getStakeholders(
          cid: LfContractId,
          source: PhysicalSynchronizerId,
      ): EitherT[FutureUnlessShutdown, TransactionRoutingError, Stakeholders] = (
        for {
          synchronizerState <- EitherT.fromEither[FutureUnlessShutdown](
            connectedSynchronizers.get(source).toRight(s"Not connected to synchronizer $source")
          )
          contract <- synchronizerState.ephemeral.contractLookup
            .lookup(cid)
            .toRight(s"Cannot find contract with id $cid")
          stakeholders = Stakeholders(contract.metadata)
        } yield stakeholders
      ).leftMap[TransactionRoutingError](AutomaticReassignmentForTransactionFailure.Failed(_))

      for {
        batches <- synchronizerRankTarget.reassignments.toSeq
          .parTraverse { case (cid, (submitter, source)) =>
            getStakeholders(cid, source)
              .map(stakeholders => (submitter, source, stakeholders, cid))
          }
          .map {
            _.groupBy { case (submitter, source, stakeholders, _cid) =>
              (submitter, source, stakeholders)
            }.view
              .mapValues(_.map { case (_submitter, _source, _stakeholders, cid) => cid })
              .toSeq
          }

        _ <- (batches: Seq[
          ((LfPartyId, PhysicalSynchronizerId, Stakeholders), Iterable[LfContractId])
        ])
          .parTraverse_ { case ((lfParty, sourceSynchronizerId, _), cids) =>
            perform(
              Source(sourceSynchronizerId),
              Target(synchronizerRankTarget.synchronizerId),
              ReassignmentSubmitterMetadata(
                submitter = lfParty,
                submittingParticipant,
                submitterInfo.commandId,
                submitterInfo.submissionId,
                submitterInfo.userId,
                workflowId = None,
              ),
              cids.toSeq,
            )
              .mapK(FutureUnlessShutdown.outcomeK)
          }
      } yield ()
    } else {
      EitherT.pure[FutureUnlessShutdown, TransactionRoutingError](())
    }

  private def perform(
      sourceSynchronizerId: Source[PhysicalSynchronizerId],
      targetSynchronizerId: Target[PhysicalSynchronizerId],
      submitterMetadata: ReassignmentSubmitterMetadata,
      contractIds: Seq[LfContractId],
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, Unit] = {
    val reassignment = for {
      sourceSynchronizer <- EitherT.fromEither[Future](
        connectedSynchronizers
          .get(sourceSynchronizerId.unwrap.logical)
          .toRight("Not connected to the source synchronizer")
      )

      targetSynchronizer <- EitherT.fromEither[Future](
        connectedSynchronizers
          .get(targetSynchronizerId.unwrap.logical)
          .toRight("Not connected to the target synchronizer")
      )

      _unit <- EitherT
        .cond[Future](
          sourceSynchronizer.ready,
          (),
          "The source synchronizer is not ready for submissions",
        )

      unassignmentResult <- sourceSynchronizer
        .submitUnassignments(submitterMetadata, contractIds, targetSynchronizerId)
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .leftMap(_.toString)
        .onShutdown(Left("Application is shutting down"))
      unassignmentStatus <- EitherT.right[String](unassignmentResult.unassignmentCompletionF)
      _unassignmentApprove <- EitherT.cond[Future](
        unassignmentStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The unassignment for ${unassignmentResult.reassignmentId} failed with status $unassignmentStatus",
      )

      _unit <- EitherT
        .cond[Future](
          targetSynchronizer.ready,
          (),
          "The target synchronizer is not ready for submission",
        )

      assignmentResult <- targetSynchronizer
        .submitAssignments(
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
