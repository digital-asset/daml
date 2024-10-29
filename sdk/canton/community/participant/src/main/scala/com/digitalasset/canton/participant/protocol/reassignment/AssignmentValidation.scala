// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.implicits.toFunctorOps
import cats.syntax.bifunctor.*
import com.daml.error.{Explanation, Resolution}
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, SyncCryptoError}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.LocalRejectionGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.EngineController.GetEngineAbortStatus
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.InvalidUnassignmentResult.DeliveredUnassignmentResultError
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.{
  AssignmentValidationResult,
  ContractDataMismatch,
  CreatingTransactionIdMismatch,
  InconsistentReassignmentCounter,
  NonInitiatorSubmitsBeforeExclusivityTimeout,
  ReassigningParticipantsMismatch,
}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.protocol.{
  ProcessingSteps,
  ReassignmentSubmissionValidation,
  SerializableContractAuthenticator,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.ReassignmentTag.Target
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}
import com.digitalasset.daml.lf.engine.Error as LfError
import com.digitalasset.daml.lf.interpretation.Error as LfInterpretationError
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

private[reassignment] class AssignmentValidation(
    domainId: Target[DomainId],
    serializableContractAuthenticator: SerializableContractAuthenticator,
    staticDomainParameters: Target[StaticDomainParameters],
    participantId: ParticipantId,
    engine: DAMLe,
    reassignmentCoordination: ReassignmentCoordination,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  def checkStakeholders(
      assignmentRequest: FullAssignmentTree,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit traceContext: TraceContext): EitherT[Future, ReassignmentProcessorError, Unit] = {
    val reassignmentId = assignmentRequest.unassignmentResultEvent.reassignmentId

    // TODO(#12926) We don't have re-interpretation check in the processing of the unassignment. Do we need it?
    val declaredContractStakeholders = Stakeholders(assignmentRequest.contract.metadata)
    val declaredViewStakeholders = assignmentRequest.stakeholders

    for {
      metadata <- engine
        .contractMetadata(
          assignmentRequest.contract.contractInstance,
          declaredContractStakeholders.all,
          getEngineAbortStatus,
        )
        .leftMap {
          case DAMLe.EngineError(
                LfError.Interpretation(
                  e @ LfError.Interpretation.DamlException(
                    LfInterpretationError.FailedAuthorization(_, _)
                  ),
                  _,
                )
              ) =>
            StakeholdersMismatch(
              Some(reassignmentId),
              declaredViewStakeholders = declaredViewStakeholders,
              declaredContractStakeholders = Some(declaredContractStakeholders),
              expectedStakeholders = Left(e.message),
            )
          case DAMLe.EngineError(error) => MetadataNotFound(error)
          case DAMLe.EngineAborted(reason) =>
            ReinterpretationAborted(reassignmentId, reason)

        }
      recomputedStakeholders = Stakeholders(metadata)
      _ <- condUnitET[Future](
        declaredViewStakeholders == recomputedStakeholders && declaredViewStakeholders == declaredContractStakeholders,
        StakeholdersMismatch(
          Some(reassignmentId),
          declaredViewStakeholders = declaredViewStakeholders,
          declaredContractStakeholders = Some(declaredContractStakeholders),
          expectedStakeholders = Right(recomputedStakeholders),
        ),
      ).leftWiden[ReassignmentProcessorError]
    } yield ()
  }

  @VisibleForTesting
  private[reassignment] def validateAssignmentRequest(
      assignmentRequestTs: CantonTimestamp,
      assignmentRequest: FullAssignmentTree,
      reassignmentDataO: Option[ReassignmentData],
      targetCrypto: Target[DomainSnapshotSyncCryptoApi],
      isReassigningParticipant: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, Option[AssignmentValidationResult]] = {
    val reassignmentId = assignmentRequest.unassignmentResultEvent.reassignmentId
    val targetSnapshot = targetCrypto.map(_.ipsSnapshot)

    reassignmentDataO match {
      case Some(reassignmentData) =>
        val sourceDomain = reassignmentData.unassignmentRequest.sourceDomain
        val unassignmentTs = reassignmentData.unassignmentTs
        for {
          sourceStaticDomainParam <- reassignmentCoordination.getStaticDomainParameter(sourceDomain)
          _ready <- {
            logger.info(
              s"Waiting for topology state at $unassignmentTs on unassignment domain $sourceDomain ..."
            )
            reassignmentCoordination
              .awaitUnassignmentTimestamp(
                sourceDomain,
                sourceStaticDomainParam,
                unassignmentTs,
              )
          }

          sourceCrypto <- reassignmentCoordination.cryptoSnapshot(
            sourceDomain,
            sourceStaticDomainParam,
            unassignmentTs,
          )

          _ <- condUnitET[Future](
            reassignmentData.unassignmentRequest.confirmingReassigningParticipants == assignmentRequest.confirmingReassigningParticipants,
            ReassigningParticipantsMismatch(
              reassignmentId,
              expected = reassignmentData.unassignmentRequest.confirmingReassigningParticipants,
              declared = assignmentRequest.confirmingReassigningParticipants,
            ),
          )

          // TODO(i12926): Validate the shipped unassignment result w.r.t. stakeholders
          // TODO(i12926): Validate that the unassignment result received matches the unassignment result in reassignmentData

          _ <- condUnitET[Future](
            assignmentRequest.contract == reassignmentData.contract,
            ContractDataMismatch(reassignmentId),
          )

          _ <- EitherT.fromEither[Future](
            serializableContractAuthenticator
              .authenticate(assignmentRequest.contract)
              .leftMap[ReassignmentProcessorError](ContractError(_))
          )

          unassignmentSubmitter = reassignmentData.unassignmentRequest.submitter
          targetTimeProof = reassignmentData.unassignmentRequest.targetTimeProof.timestamp

          // TODO(i12926): Check that reassignmentData.unassignmentRequest.targetTimeProof.timestamp is in the past
          cryptoSnapshotAtTimeProof <- reassignmentCoordination
            .cryptoSnapshot(
              reassignmentData.targetDomain,
              staticDomainParameters,
              targetTimeProof,
            )

          _ <- DeliveredUnassignmentResultValidation(
            unassignmentRequest = reassignmentData.unassignmentRequest,
            unassignmentRequestTs = reassignmentData.unassignmentTs,
            unassignmentDecisionTime = reassignmentData.unassignmentDecisionTime,
            sourceTopology = sourceCrypto,
            targetTopology = targetSnapshot,
          )(assignmentRequest.unassignmentResultEvent).validate
            .leftMap(err => DeliveredUnassignmentResultError(reassignmentId, err.error).reported())

          // TODO(#12926): validate assignmentRequest.stakeholders

          _ <- ReassignmentSubmissionValidation.assignment(
            reassignmentId = reassignmentId,
            topologySnapshot = targetSnapshot,
            submitter = assignmentRequest.submitter,
            participantId = assignmentRequest.submitterMetadata.submittingParticipant,
            stakeholders = assignmentRequest.stakeholders.all,
          )

          exclusivityLimit <- ProcessingSteps
            .getAssignmentExclusivity(
              cryptoSnapshotAtTimeProof.map(_.ipsSnapshot),
              targetTimeProof,
            )
            .leftMap[ReassignmentProcessorError](ReassignmentParametersError(domainId.unwrap, _))

          _ <- condUnitET[Future](
            assignmentRequestTs >= exclusivityLimit.unwrap || unassignmentSubmitter == assignmentRequest.submitter,
            NonInitiatorSubmitsBeforeExclusivityTimeout(
              reassignmentId,
              assignmentRequest.submitter,
              currentTimestamp = assignmentRequestTs,
              timeout = exclusivityLimit,
            ),
          )

          _ <- condUnitET[Future](
            reassignmentData.creatingTransactionId == assignmentRequest.creatingTransactionId,
            CreatingTransactionIdMismatch(
              reassignmentId,
              assignmentRequest.creatingTransactionId,
              reassignmentData.creatingTransactionId,
            ),
          )
          sourceIps = sourceCrypto.map(_.ipsSnapshot)

          stakeholders = assignmentRequest.stakeholders
          sourceConfirmingParties <- EitherT.right(
            sourceIps.unwrap.canConfirm(participantId, stakeholders.all)
          )
          targetConfirmingParties <- EitherT.right(
            targetSnapshot.unwrap.canConfirm(participantId, stakeholders.all)
          )
          confirmingParties = sourceConfirmingParties.intersect(targetConfirmingParties)

          _ <- EitherT.cond[Future](
            // reassignment counter is the same in unassignment and assignment requests
            assignmentRequest.reassignmentCounter == reassignmentData.reassignmentCounter,
            (),
            InconsistentReassignmentCounter(
              reassignmentId,
              assignmentRequest.reassignmentCounter,
              reassignmentData.reassignmentCounter,
            ): ReassignmentProcessorError,
          )

        } yield Some(AssignmentValidationResult(confirmingParties))

      case None =>
        // TODO(#12926) Check what validations can be done here + ensure coverage
        for {
          _ <- ReassignmentSubmissionValidation.assignment(
            reassignmentId = reassignmentId,
            topologySnapshot = targetSnapshot,
            submitter = assignmentRequest.submitter,
            participantId = assignmentRequest.submitterMetadata.submittingParticipant,
            stakeholders = assignmentRequest.stakeholders.all,
          )

          confirmingPartiesF = targetSnapshot.unwrap
            .canConfirm(
              participantId,
              assignmentRequest.stakeholders.all,
            )

          _ <- EitherT.fromEither[Future](
            serializableContractAuthenticator
              .authenticate(assignmentRequest.contract)
              .leftMap[ReassignmentProcessorError](ContractError(_))
          )

          confirmingParties <- EitherT
            .liftF[Future, ReassignmentProcessorError, Set[LfPartyId]](confirmingPartiesF)

          res <-
            if (isReassigningParticipant) {
              // This happens either in case of malicious assignments (incorrectly declared reassigning participants)
              // OR if the reassignment data has been pruned.
              // The assignment should be rejected due to other validations (e.g. conflict detection), but
              // we could code this more defensively at some point
              EitherT.rightT[Future, ReassignmentProcessorError](
                Some(AssignmentValidationResult(confirmingParties))
              )
            } else EitherT.rightT[Future, ReassignmentProcessorError](None)
        } yield res
    }
  }
}

object AssignmentValidation extends LocalRejectionGroup {
  @Explanation(
    """The mediator has received an invalid delivered unassignment result.
      |This may occur due to a bug at the sender of the message."""
  )
  @Resolution("Contact support.")
  object InvalidUnassignmentResult
      extends AlarmErrorCode("PARTICIPANT_RECEIVED_INVALID_DELIVERED_UNASSIGNMENT_RESULT") {
    final case class DeliveredUnassignmentResultError(
        override val cause: String
    ) extends Alarm(cause)
        with AssignmentValidationError {
      override def message: String = cause
    }

    object DeliveredUnassignmentResultError {
      def apply(reassignmentId: ReassignmentId, error: String): DeliveredUnassignmentResultError =
        DeliveredUnassignmentResultError(
          s"Cannot assign `$reassignmentId`: validation of DeliveredUnassignmentResult failed with error: $error"
        )
    }
  }

  final case class AssignmentValidationResult(confirmingParties: Set[LfPartyId])

  private[reassignment] sealed trait AssignmentValidationError extends ReassignmentProcessorError

  final case class NoReassignmentData(
      reassignmentId: ReassignmentId,
      lookupError: ReassignmentStore.ReassignmentLookupError,
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot find reassignment data for reassignment `$reassignmentId`: ${lookupError.cause}"
  }

  final case class UnassignmentIncomplete(
      reassignmentId: ReassignmentId,
      participant: ParticipantId,
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign `$reassignmentId` because unassignment is incomplete"
  }

  final case class NoParticipantForReceivingParty(reassignmentId: ReassignmentId, party: LfPartyId)
      extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign `$reassignmentId` because $party is not active"
  }

  final case class UnexpectedDomain(
      reassignmentId: ReassignmentId,
      targetDomain: DomainId,
      receivedOn: DomainId,
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: expecting domain `$targetDomain` but received on `$receivedOn`"
  }

  final case class ReassigningParticipantsMismatch(
      reassignmentId: ReassignmentId,
      expected: Set[ParticipantId],
      declared: Set[ParticipantId],
  ) extends UnassignmentProcessorError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: reassigning participants mismatch"
  }

  final case class NonInitiatorSubmitsBeforeExclusivityTimeout(
      reassignmentId: ReassignmentId,
      submitter: LfPartyId,
      currentTimestamp: CantonTimestamp,
      timeout: Target[CantonTimestamp],
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: only submitter can initiate before exclusivity timeout $timeout"
  }

  final case class ContractDataMismatch(reassignmentId: ReassignmentId)
      extends AssignmentValidationError {
    override def message: String = s"Cannot assign `$reassignmentId`: contract data mismatch"
  }

  final case class CreatingTransactionIdMismatch(
      reassignmentId: ReassignmentId,
      assignmentTransactionId: TransactionId,
      localTransactionId: TransactionId,
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: creating transaction id mismatch"
  }

  final case class InconsistentReassignmentCounter(
      reassignmentId: ReassignmentId,
      declaredReassignmentCounter: ReassignmentCounter,
      expectedReassignmentCounter: ReassignmentCounter,
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign $reassignmentId: reassignment counter $declaredReassignmentCounter in assignment does not match $expectedReassignmentCounter from the unassignment"
  }

  final case class ReassignmentSigningError(
      cause: SyncCryptoError
  ) extends ReassignmentProcessorError {
    override def message: String = show"Unable to sign reassignment request. $cause"
  }
}
