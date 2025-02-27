// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.{CantonBaseError, MediatorError}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.NoReassignmentData
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.reassignment.UnassignmentProcessorError.AutomaticAssignmentError
import com.digitalasset.canton.participant.store.ReassignmentStore.ReassignmentCompleted
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext

private[participant] object AutomaticAssignment {
  def perform(
      id: ReassignmentId,
      targetSynchronizer: Target[SynchronizerId],
      targetStaticSynchronizerParameters: Target[StaticSynchronizerParameters],
      reassignmentCoordination: ReassignmentCoordination,
      stakeholders: Set[LfPartyId],
      unassignmentSubmitterMetadata: ReassignmentSubmitterMetadata,
      participantId: ParticipantId,
      t0: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
    val logger = elc.logger
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

    def performAutoAssignmentOnce
        : EitherT[FutureUnlessShutdown, ReassignmentProcessorError, com.google.rpc.status.Status] =
      for {
        targetIps <- reassignmentCoordination
          .getTimeProofAndSnapshot(targetSynchronizer, targetStaticSynchronizerParameters)
          .map(_._2)
        possibleSubmittingParties <- EitherT.right(hostedStakeholders(targetIps.map(_.ipsSnapshot)))
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
              unassignmentSubmitterMetadata.applicationId,
              workflowId = None,
            ),
            id,
          )(TraceContext.empty)
          .mapK(FutureUnlessShutdown.outcomeK)
        AssignmentProcessingSteps.SubmissionResult(completionF) = submissionResult
        status <- EitherT.right(completionF).mapK(FutureUnlessShutdown.outcomeK)
      } yield status

    def performAutoAssignmentRepeatedly
        : EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] = {
      final case class StopRetry(
          result: Either[ReassignmentProcessorError, com.google.rpc.status.Status]
      )
      val retryCount = 5

      def tryAgain(
          previous: com.google.rpc.status.Status
      ): EitherT[FutureUnlessShutdown, StopRetry, com.google.rpc.status.Status] =
        if (CantonBaseError.isStatusErrorCode(MediatorError.Timeout, previous))
          performAutoAssignmentOnce.leftMap(error => StopRetry(Left(error)))
        else
          EitherT
            .leftT[FutureUnlessShutdown, com.google.rpc.status.Status](StopRetry(Right(previous)))

      val initial = performAutoAssignmentOnce.leftMap(error => StopRetry(Left(error)))
      val result = MonadUtil.repeatFlatmap(initial, tryAgain, retryCount)

      // The status was only useful to understand whether the operation could be retried
      result
        .leftFlatMap(attempt => EitherT.fromEither[FutureUnlessShutdown](attempt.result))
        .map(_.discard)
    }

    def triggerAutoAssignment(
        targetSnapshot: Target[TopologySnapshot],
        targetSynchronizerParameters: Target[DynamicSynchronizerParametersWithValidity],
    ): Unit = {

      val autoAssignment = for {
        exclusivityLimit <- EitherT
          .fromEither[FutureUnlessShutdown](
            targetSynchronizerParameters.unwrap
              .assignmentExclusivityLimitFor(t0)
              .leftMap(ReassignmentParametersError(targetSynchronizer.unwrap, _))
          )
          .leftWiden[ReassignmentProcessorError]

        targetHostedStakeholders <- EitherT.right(hostedStakeholders(targetSnapshot))
        _ <-
          if (targetHostedStakeholders.nonEmpty) {
            logger.info(
              s"Registering automatic submission of assignment with ID $id at time $exclusivityLimit, where base timestamp is $t0"
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
                case UnknownSynchronizer(synchronizer, _)
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
          t0,
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
