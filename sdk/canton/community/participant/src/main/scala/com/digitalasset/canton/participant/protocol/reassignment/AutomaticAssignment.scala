// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata}
import com.digitalasset.canton.error.CantonBaseError.isStatusErrorCode
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{ErrorLoggingContext, TracedLogger}
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.NoReassignmentData
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.AutomaticAssignmentError
import com.digitalasset.canton.participant.store.ReassignmentStore.ReassignmentCompleted
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.util.retry.{Backoff, NoExceptionRetryPolicy, Success}
import com.google.rpc.Code
import com.google.rpc.status.Status
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

private[participant] object AutomaticAssignment {
  def perform(
      id: ReassignmentId,
      targetSynchronizer: Target[PhysicalSynchronizerId],
      targetStaticSynchronizerParameters: Target[StaticSynchronizerParameters],
      reassignmentCoordination: ReassignmentCoordination,
      stakeholders: Set[LfPartyId],
      unassignmentSubmitterMetadata: ReassignmentSubmitterMetadata,
      participantId: ParticipantId,
      targetTimestamp: Target[CantonTimestamp],
  )(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val logger = elc
    implicit val traceContext: TraceContext = elc.traceContext

    def hostedStakeholders(
        snapshot: Target[TopologySnapshot]
    ): FutureUnlessShutdown[Set[LfPartyId]] =
      snapshot.unwrap
        .hostedOn(stakeholders, participantId)
        .map(partiesWithAttributes =>
          partiesWithAttributes.collect {
            case (partyId, attributes)
                if attributes.permission == ParticipantPermission.Submission =>
              partyId
          }.toSet
        )

    def performAutoAssignmentOnce: EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
      for {
        targetTopology <- reassignmentCoordination
          .getRecentTopologySnapshot(
            targetSynchronizer,
            targetStaticSynchronizerParameters,
          )
        possibleSubmittingParties <- EitherT.right(hostedStakeholders(targetTopology))
        assignmentSubmitter <- EitherT.fromOption[FutureUnlessShutdown](
          possibleSubmittingParties.headOption,
          AutomaticAssignmentError("No possible submitting party for automatic assignment"),
        )
        submissionResult <- reassignmentCoordination
          .assign(
            targetSynchronizer,
            ReassignmentSubmitterMetadata(
              assignmentSubmitter,
              participantId,
              unassignmentSubmitterMetadata.commandId,
              submissionId = None,
              unassignmentSubmitterMetadata.userId,
              workflowId = None,
            ),
            id,
            targetTopology,
          )(TraceContext.empty)
          .mapK(FutureUnlessShutdown.outcomeK)
        AssignmentProcessingSteps.SubmissionResult(completionF) = submissionResult
        status <- EitherT.right(completionF).mapK(FutureUnlessShutdown.outcomeK)
        _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
          status.code == Code.OK_VALUE,
          AssignmentFailed(status): ReassignmentProcessorError,
        )
      } yield ()

    final case class AssignmentFailed(status: Status) extends ReassignmentProcessorError {
      def message = s"Assignment of $id failed due to: $status"
    }

    def performAutoAssignmentRepeatedly
        : EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
      implicit val StopRetry: Success[Either[ReassignmentProcessorError, Unit]] = Success {
        case Left(e: AssignmentFailed) => !isStatusErrorCode(MediatorError.Timeout, e.status)
        case _ => true
      }

      case object SyncWithClosing extends FlagCloseable with HasCloseContext {
        override val timeouts: ProcessingTimeout = ProcessingTimeout()
        override val logger: TracedLogger = elc.logger
      }

      val retry = Backoff(
        elc.logger,
        hasSynchronizeWithClosing = SyncWithClosing,
        maxRetries = 5,
        initialDelay = 1.second,
        maxDelay = 10.seconds,
        operationName = s"automatic assignment of $id",
      )
      EitherT(retry.unlessShutdown(performAutoAssignmentOnce.value, NoExceptionRetryPolicy))
    }

    def triggerAutoAssignment(
        targetSnapshot: Target[TopologySnapshot],
        targetSynchronizerParameters: Target[DynamicSynchronizerParametersWithValidity],
    ): Unit = {

      val autoAssignment = for {
        exclusivityLimit <- EitherT
          .fromEither[FutureUnlessShutdown](
            targetSynchronizerParameters.unwrap
              .assignmentExclusivityLimitFor(targetTimestamp.unwrap)
              .map(Target(_))
              .leftMap(ReassignmentParametersError(targetSynchronizer.unwrap, _))
          )
          .leftWiden[ReassignmentProcessorError]

        targetHostedStakeholders <- EitherT.right(hostedStakeholders(targetSnapshot))
        _ <-
          if (targetHostedStakeholders.nonEmpty) {
            logger.info(
              s"Registering automatic submission of assignment with ID $id at time $exclusivityLimit, where base timestamp is $targetTimestamp"
            )
            for {
              _ <- reassignmentCoordination
                .awaitSynchronizerTime(targetSynchronizer, exclusivityLimit)
                .mapK(FutureUnlessShutdown.outcomeK)
              _ <- reassignmentCoordination
                .awaitTimestamp(
                  targetSynchronizer,
                  targetStaticSynchronizerParameters,
                  exclusivityLimit,
                  FutureUnlessShutdown.pure(
                    logger.debug(s"Automatic assignment triggered immediately")
                  ),
                )

              _ <- EitherTUtil.leftSubflatMap(performAutoAssignmentRepeatedly) {
                // Filter out submission errors occurring because the reassignment is already completed
                case NoReassignmentData(_, ReassignmentCompleted(_, _)) =>
                  Either.unit
                // Filter out the case that the participant has disconnected from the target synchronizer in the meantime.
                case UnknownPhysicalSynchronizer(synchronizer, _)
                    if synchronizer == targetSynchronizer.unwrap =>
                  Either.unit
                case SynchronizerNotReady(synchronizer, _)
                    if synchronizer == targetSynchronizer.unwrap =>
                  Either.unit
                // Filter out the case that the target synchronizer is closing right now
                case other => Left(other)
              }
            } yield ()
          } else EitherT.pure[FutureUnlessShutdown, ReassignmentProcessorError](())
      } yield ()

      EitherTUtil.doNotAwaitUS(autoAssignment, "Automatic assignment failed", Level.INFO)
    }

    for {
      targetIps <- reassignmentCoordination
        .cryptoSnapshot(
          targetSynchronizer,
          targetStaticSynchronizerParameters,
          targetTimestamp,
        )

      targetSnapshot = targetIps.map(_.ipsSnapshot)

      targetSynchronizerParameters <- EitherT(
        targetSnapshot
          .traverse(
            _.findDynamicSynchronizerParameters()
              .map(_.leftMap(SynchronizerNotReady(targetSynchronizer.unwrap, _)))
          )
          .map(_.sequence)
      ).leftWiden[ReassignmentProcessorError]
    } yield {

      if (targetSynchronizerParameters.unwrap.automaticAssignmentEnabled)
        triggerAutoAssignment(targetSnapshot, targetSynchronizerParameters)
      else ()
    }
  }
}
