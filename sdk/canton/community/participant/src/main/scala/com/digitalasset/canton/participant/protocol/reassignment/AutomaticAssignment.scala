// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.*
import cats.syntax.bifunctor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.{BaseCantonError, MediatorError}
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
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

private[participant] object AutomaticAssignment {
  def perform(
      id: ReassignmentId,
      targetDomain: TargetDomainId,
      staticDomainParameters: StaticDomainParameters,
      reassignmentCoordination: ReassignmentCoordination,
      stakeholders: Set[LfPartyId],
      unassignmentSubmitterMetadata: ReassignmentSubmitterMetadata,
      participantId: ParticipantId,
      t0: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[Future, ReassignmentProcessorError, Unit] = {
    val logger = elc.logger
    implicit val traceContext: TraceContext = elc.traceContext

    def hostedStakeholders(snapshot: TopologySnapshot): Future[Set[LfPartyId]] =
      snapshot
        .hostedOn(stakeholders, participantId)
        .map(partiesWithAttributes =>
          partiesWithAttributes.collect {
            case (partyId, attributes)
                if attributes.permission == ParticipantPermission.Submission =>
              partyId
          }.toSet
        )

    def performAutoInOnce
        : EitherT[Future, ReassignmentProcessorError, com.google.rpc.status.Status] =
      for {
        targetIps <- reassignmentCoordination
          .getTimeProofAndSnapshot(targetDomain, staticDomainParameters)
          .map(_._2)
          .onShutdown(Left(DomainNotReady(targetDomain.unwrap, "Shutdown of time tracker")))
        possibleSubmittingParties <- EitherT.right(hostedStakeholders(targetIps.ipsSnapshot))
        inParty <- EitherT.fromOption[Future](
          possibleSubmittingParties.headOption,
          AutomaticAssignmentError("No possible submitting party for automatic assignment"),
        )
        submissionResult <- reassignmentCoordination
          .assign(
            targetDomain,
            ReassignmentSubmitterMetadata(
              inParty,
              participantId,
              unassignmentSubmitterMetadata.commandId,
              submissionId = None,
              unassignmentSubmitterMetadata.applicationId,
              workflowId = None,
            ),
            id,
          )(
            TraceContext.empty
          )
        AssignmentProcessingSteps.SubmissionResult(completionF) = submissionResult
        status <- EitherT.right(completionF)
      } yield status

    def performAutoInRepeatedly: EitherT[Future, ReassignmentProcessorError, Unit] = {
      final case class StopRetry(
          result: Either[ReassignmentProcessorError, com.google.rpc.status.Status]
      )
      val retryCount = 5

      def tryAgain(
          previous: com.google.rpc.status.Status
      ): EitherT[Future, StopRetry, com.google.rpc.status.Status] =
        if (BaseCantonError.isStatusErrorCode(MediatorError.Timeout, previous))
          performAutoInOnce.leftMap(error => StopRetry(Left(error)))
        else EitherT.leftT[Future, com.google.rpc.status.Status](StopRetry(Right(previous)))

      val initial = performAutoInOnce.leftMap(error => StopRetry(Left(error)))
      val result = MonadUtil.repeatFlatmap(initial, tryAgain, retryCount)

      // The status was only useful to understand whether the operation could be retried
      result.leftFlatMap(attempt => EitherT.fromEither[Future](attempt.result)).map(_.discard)
    }

    def triggerAutoIn(
        targetSnapshot: TopologySnapshot,
        targetDomainParameters: DynamicDomainParametersWithValidity,
    ): Unit = {

      val autoIn = for {
        exclusivityLimit <- EitherT
          .fromEither[Future](
            targetDomainParameters
              .assignmentExclusivityLimitFor(t0)
              .leftMap(ReassignmentParametersError(targetDomain.unwrap, _))
          )
          .leftWiden[ReassignmentProcessorError]

        targetHostedStakeholders <- EitherT.right(hostedStakeholders(targetSnapshot))
        _ <-
          if (targetHostedStakeholders.nonEmpty) {
            logger.info(
              s"Registering automatic submission of assignment with ID $id at time $exclusivityLimit, where base timestamp is $t0"
            )
            for {
              _ <- reassignmentCoordination.awaitDomainTime(targetDomain.unwrap, exclusivityLimit)
              _ <- reassignmentCoordination.awaitTimestamp(
                targetDomain.unwrap,
                staticDomainParameters,
                exclusivityLimit,
                Future.successful(logger.debug(s"Automatic assignment triggered immediately")),
              )

              _ <- EitherTUtil.leftSubflatMap(performAutoInRepeatedly) {
                // Filter out submission errors occurring because the reassignment is already completed
                case NoReassignmentData(_, ReassignmentCompleted(_, _)) =>
                  Right(())
                // Filter out the case that the participant has disconnected from the target domain in the meantime.
                case UnknownDomain(domain, _) if domain == targetDomain.unwrap =>
                  Right(())
                case DomainNotReady(domain, _) if domain == targetDomain.unwrap =>
                  Right(())
                // Filter out the case that the target domain is closing right now
                case other => Left(other)
              }
            } yield ()
          } else EitherT.pure[Future, ReassignmentProcessorError](())
      } yield ()

      EitherTUtil.doNotAwait(autoIn, "Automatic assignment failed", Level.INFO)
    }

    for {
      targetIps <- reassignmentCoordination.cryptoSnapshot(
        targetDomain.unwrap,
        staticDomainParameters,
        t0,
      )
      targetSnapshot = targetIps.ipsSnapshot

      targetDomainParameters <- EitherT(
        targetSnapshot
          .findDynamicDomainParameters()
          .map(_.leftMap(DomainNotReady(targetDomain.unwrap, _)))
      ).leftWiden[ReassignmentProcessorError]
    } yield {

      if (targetDomainParameters.automaticAssignmentEnabled)
        triggerAutoIn(targetSnapshot, targetDomainParameters)
      else ()
    }
  }
}
