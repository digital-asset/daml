// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, SyncCryptoError}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.EngineController.GetEngineAbortStatus
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.reassignment.AssignmentValidation.*
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.*
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.EitherUtil.condUnitE
import com.digitalasset.canton.{LfPartyId, ReassignmentCounter}
import com.digitalasset.daml.lf.engine.Error as LfError
import com.digitalasset.daml.lf.interpretation.Error as LfInterpretationError
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

private[reassignment] class AssignmentValidation(
    domainId: TargetDomainId,
    staticDomainParameters: StaticDomainParameters,
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

    val declaredContractStakeholders = assignmentRequest.contract.metadata.stakeholders
    val declaredViewStakeholders = assignmentRequest.stakeholders

    for {
      metadata <- engine
        .contractMetadata(
          assignmentRequest.contract.contractInstance,
          declaredContractStakeholders,
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
      recomputedStakeholders = metadata.stakeholders
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
      targetCrypto: DomainSnapshotSyncCryptoApi,
      isReassigningParticipant: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, Option[AssignmentValidationResult]] = {
    val unassignmentResultEvent = assignmentRequest.unassignmentResultEvent.result

    val reassignmentId = assignmentRequest.unassignmentResultEvent.reassignmentId

    def checkSubmitterIsStakeholder: Either[ReassignmentProcessorError, Unit] =
      condUnitE(
        assignmentRequest.stakeholders.contains(assignmentRequest.submitter),
        AssignmentSubmitterMustBeStakeholder(
          reassignmentId,
          assignmentRequest.submitter,
          assignmentRequest.stakeholders,
        ),
      )

    val targetIps = targetCrypto.ipsSnapshot

    reassignmentDataO match {
      case Some(reassignmentData) =>
        val sourceDomain = reassignmentData.unassignmentRequest.sourceDomain
        val unassignmentTs = reassignmentData.unassignmentTs
        for {
          _ready <- {
            logger.info(
              s"Waiting for topology state at $unassignmentTs on unassignment domain $sourceDomain ..."
            )
            EitherT(
              reassignmentCoordination
                .awaitUnassignmentTimestamp(
                  sourceDomain,
                  staticDomainParameters,
                  unassignmentTs,
                )
                .sequence
            )
          }

          sourceCrypto <- reassignmentCoordination.cryptoSnapshot(
            sourceDomain.unwrap,
            staticDomainParameters,
            unassignmentTs,
          )
          // TODO(i12926): Check the signatures of the mediator and the sequencer

          _ <- condUnitET[Future](
            unassignmentResultEvent.content.timestamp <= reassignmentData.unassignmentDecisionTime,
            ResultTimestampExceedsDecisionTime(
              reassignmentId,
              timestamp = unassignmentResultEvent.content.timestamp,
              decisionTime = reassignmentData.unassignmentDecisionTime,
            ),
          )

          _ <- condUnitET[Future](
            reassignmentData.unassignmentRequest.reassigningParticipants == assignmentRequest.reassigningParticipants,
            ReassigningParticipantsMismatch(
              reassignmentId,
              expected = reassignmentData.unassignmentRequest.reassigningParticipants,
              declared = assignmentRequest.reassigningParticipants,
            ),
          )

          // TODO(i12926): Validate the shipped unassignment result w.r.t. stakeholders
          // TODO(i12926): Validate that the unassignment result received matches the unassignment result in reassignmentData

          _ <- condUnitET[Future](
            assignmentRequest.contract == reassignmentData.contract,
            ContractDataMismatch(reassignmentId),
          )
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          unassignmentSubmitter = reassignmentData.unassignmentRequest.submitter
          targetTimeProof = reassignmentData.unassignmentRequest.targetTimeProof.timestamp

          // TODO(i12926): Check that reassignmentData.unassignmentRequest.targetTimeProof.timestamp is in the past
          cryptoSnapshot <- reassignmentCoordination
            .cryptoSnapshot(
              reassignmentData.targetDomain.unwrap,
              staticDomainParameters,
              targetTimeProof,
            )

          exclusivityLimit <- ProcessingSteps
            .getAssignmentExclusivity(
              cryptoSnapshot.ipsSnapshot,
              targetTimeProof,
            )
            .leftMap[ReassignmentProcessorError](ReassignmentParametersError(domainId.unwrap, _))

          _ <- condUnitET[Future](
            assignmentRequestTs >= exclusivityLimit || unassignmentSubmitter == assignmentRequest.submitter,
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
          sourceIps = sourceCrypto.ipsSnapshot

          stakeholders = assignmentRequest.stakeholders
          sourceConfirmingParties <- EitherT.right(
            sourceIps.canConfirm(participantId, stakeholders)
          )
          targetConfirmingParties <- EitherT.right(
            targetIps.canConfirm(participantId, stakeholders)
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
        for {
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          res <-
            if (isReassigningParticipant) {
              // This happens either in case of malicious assignments (incorrectly declared reassigning participants)
              // OR if the reassignment data has been pruned.
              // The assignment should be rejected due to other validations (e.g. conflict detection), but
              // we could code this more defensively at some point

              val targetIps = targetCrypto.ipsSnapshot
              val confirmingPartiesF = targetIps
                .canConfirm(
                  participantId,
                  assignmentRequest.stakeholders,
                )
              EitherT(confirmingPartiesF.map { confirmingParties =>
                Right(Some(AssignmentValidationResult(confirmingParties))): Either[
                  ReassignmentProcessorError,
                  Option[AssignmentValidationResult],
                ]
              })
            } else EitherT.rightT[Future, ReassignmentProcessorError](None)
        } yield res
    }
  }
}

object AssignmentValidation {
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

  final case class ResultTimestampExceedsDecisionTime(
      reassignmentId: ReassignmentId,
      timestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot assign `$reassignmentId`: result time $timestamp exceeds decision time $decisionTime"
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
      timeout: CantonTimestamp,
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
