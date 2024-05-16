// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.*
import cats.syntax.bifunctor.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.{BaseCantonError, MediatorError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.protocol.transfer.TransferInValidation.NoTransferData
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessorError.AutomaticTransferInError
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.store.TransferStore.TransferCompleted
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

private[participant] object AutomaticTransferIn {
  def perform(
      id: TransferId,
      targetDomain: TargetDomainId,
      transferCoordination: TransferCoordination,
      stakeholders: Set[LfPartyId],
      transferOutSubmitterMetadata: TransferSubmitterMetadata,
      participantId: ParticipantId,
      t0: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[Future, TransferProcessorError, Unit] = {
    val logger = elc.logger
    implicit val traceContext: TraceContext = elc.traceContext

    def hostedStakeholders(snapshot: TopologySnapshot): Future[Set[LfPartyId]] = {
      snapshot
        .hostedOn(stakeholders, participantId)
        .map(partiesWithAttributes =>
          partiesWithAttributes.collect {
            case (partyId, attributes)
                if attributes.permission == ParticipantPermission.Submission =>
              partyId
          }.toSet
        )

    }

    def performAutoInOnce: EitherT[Future, TransferProcessorError, com.google.rpc.status.Status] = {
      for {
        targetIps <- transferCoordination
          .getTimeProofAndSnapshot(targetDomain)
          .map(_._2)
          .onShutdown(Left(DomainNotReady(targetDomain.unwrap, "Shutdown of time tracker")))
        possibleSubmittingParties <- EitherT.right(hostedStakeholders(targetIps.ipsSnapshot))
        inParty <- EitherT.fromOption[Future](
          possibleSubmittingParties.headOption,
          AutomaticTransferInError("No possible submitting party for automatic transfer-in"),
        )
        sourceProtocolVersion <- EitherT
          .fromEither[Future](
            transferCoordination
              .protocolVersionFor(Traced(id.sourceDomain.unwrap))
              .toRight(
                AutomaticTransferInError(
                  s"Unable to get protocol version of source domain ${id.sourceDomain}"
                )
              )
          )
          .map(SourceProtocolVersion(_))
        submissionResult <- transferCoordination
          .transferIn(
            targetDomain,
            TransferSubmitterMetadata(
              inParty,
              participantId,
              transferOutSubmitterMetadata.commandId,
              submissionId = None,
              transferOutSubmitterMetadata.applicationId,
              workflowId = None,
            ),
            id,
            sourceProtocolVersion,
          )(
            TraceContext.empty
          )
        TransferInProcessingSteps.SubmissionResult(completionF) = submissionResult
        status <- EitherT.liftF(completionF)
      } yield status
    }

    def performAutoInRepeatedly: EitherT[Future, TransferProcessorError, Unit] = {
      final case class StopRetry(
          result: Either[TransferProcessorError, com.google.rpc.status.Status]
      )
      val retryCount = 5

      def tryAgain(
          previous: com.google.rpc.status.Status
      ): EitherT[Future, StopRetry, com.google.rpc.status.Status] = {
        if (BaseCantonError.isStatusErrorCode(MediatorError.Timeout, previous))
          performAutoInOnce.leftMap(error => StopRetry(Left(error)))
        else EitherT.leftT[Future, com.google.rpc.status.Status](StopRetry(Right(previous)))
      }

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
              .transferExclusivityLimitFor(t0)
              .leftMap(TransferParametersError(targetDomain.unwrap, _))
          )
          .leftWiden[TransferProcessorError]

        targetHostedStakeholders <- EitherT.right(hostedStakeholders(targetSnapshot))
        _ <-
          if (targetHostedStakeholders.nonEmpty) {
            logger.info(
              s"Registering automatic submission of transfer-in with ID $id at time $exclusivityLimit, where base timestamp is $t0"
            )
            for {
              _ <- transferCoordination.awaitTimestamp(
                targetDomain.unwrap,
                exclusivityLimit,
                waitForEffectiveTime = false,
                Future.successful(logger.debug(s"Automatic transfer-in triggered immediately")),
              )

              _ <- EitherTUtil.leftSubflatMap(performAutoInRepeatedly) {
                // Filter out submission errors occurring because the transfer is already completed
                case NoTransferData(_, TransferCompleted(_, _)) =>
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
          } else EitherT.pure[Future, TransferProcessorError](())
      } yield ()

      EitherTUtil.doNotAwait(autoIn, "Automatic transfer-in failed", Level.INFO)
    }

    for {
      targetIps <- transferCoordination.cryptoSnapshot(targetDomain.unwrap, t0)
      targetSnapshot = targetIps.ipsSnapshot

      targetDomainParameters <- EitherT(
        targetSnapshot
          .findDynamicDomainParameters()
          .map(_.leftMap(DomainNotReady(targetDomain.unwrap, _)))
      ).leftWiden[TransferProcessorError]
    } yield {

      if (targetDomainParameters.automaticTransferInEnabled)
        triggerAutoIn(targetSnapshot, targetDomainParameters)
      else ()
    }
  }
}
