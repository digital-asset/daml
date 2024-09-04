// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, SyncCryptoError}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.EngineController.GetEngineAbortStatus
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.participant.protocol.transfer.AssignmentValidation.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
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

private[transfer] class AssignmentValidation(
    domainId: TargetDomainId,
    staticDomainParameters: StaticDomainParameters,
    participantId: ParticipantId,
    engine: DAMLe,
    transferCoordination: TransferCoordination,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  def checkStakeholders(
      assignmentRequest: FullAssignmentTree,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Unit] = {
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
      ).leftWiden[TransferProcessorError]
    } yield ()
  }

  @VisibleForTesting
  private[transfer] def validateAssignmentRequest(
      tsIn: CantonTimestamp,
      assignmentRequest: FullAssignmentTree,
      transferDataO: Option[TransferData],
      targetCrypto: DomainSnapshotSyncCryptoApi,
      isReassigningParticipant: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Option[AssignmentValidationResult]] = {
    val txOutResultEvent = assignmentRequest.unassignmentResultEvent.result

    val reassignmentId = assignmentRequest.unassignmentResultEvent.reassignmentId

    def checkSubmitterIsStakeholder: Either[TransferProcessorError, Unit] =
      condUnitE(
        assignmentRequest.stakeholders.contains(assignmentRequest.submitter),
        SubmittingPartyMustBeStakeholderIn(
          reassignmentId,
          assignmentRequest.submitter,
          assignmentRequest.stakeholders,
        ),
      )

    val targetIps = targetCrypto.ipsSnapshot

    transferDataO match {
      case Some(transferData) =>
        val sourceDomain = transferData.unassignmentRequest.sourceDomain
        val unassignmentTs = transferData.unassignmentTs
        for {
          _ready <- {
            logger.info(
              s"Waiting for topology state at $unassignmentTs on unassignment domain $sourceDomain ..."
            )
            EitherT(
              transferCoordination
                .awaitUnassignmentTimestamp(
                  sourceDomain,
                  staticDomainParameters,
                  unassignmentTs,
                )
                .sequence
            )
          }

          sourceCrypto <- transferCoordination.cryptoSnapshot(
            sourceDomain.unwrap,
            staticDomainParameters,
            unassignmentTs,
          )
          // TODO(i12926): Check the signatures of the mediator and the sequencer

          _ <- condUnitET[Future](
            txOutResultEvent.content.timestamp <= transferData.unassignmentDecisionTime,
            ResultTimestampExceedsDecisionTime(
              reassignmentId,
              timestamp = txOutResultEvent.content.timestamp,
              decisionTime = transferData.unassignmentDecisionTime,
            ),
          )

          // TODO(i12926): Validate the shipped unassignment result w.r.t. stakeholders
          // TODO(i12926): Validate that the unassignment result received matches the unassignment result in transferData

          _ <- condUnitET[Future](
            assignmentRequest.contract == transferData.contract,
            ContractDataMismatch(reassignmentId),
          )
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          unassignmentSubmitter = transferData.unassignmentRequest.submitter
          targetTimeProof = transferData.unassignmentRequest.targetTimeProof.timestamp

          // TODO(i12926): Check that transferData.unassignmentRequest.targetTimeProof.timestamp is in the past
          cryptoSnapshot <- transferCoordination
            .cryptoSnapshot(
              transferData.targetDomain.unwrap,
              staticDomainParameters,
              targetTimeProof,
            )

          exclusivityLimit <- ProcessingSteps
            .getAssignmentExclusivity(
              cryptoSnapshot.ipsSnapshot,
              targetTimeProof,
            )
            .leftMap[TransferProcessorError](TransferParametersError(domainId.unwrap, _))

          _ <- condUnitET[Future](
            tsIn >= exclusivityLimit
              || unassignmentSubmitter == assignmentRequest.submitter,
            NonInitiatorSubmitsBeforeExclusivityTimeout(
              reassignmentId,
              assignmentRequest.submitter,
              currentTimestamp = tsIn,
              timeout = exclusivityLimit,
            ),
          )
          _ <- condUnitET[Future](
            transferData.creatingTransactionId == assignmentRequest.creatingTransactionId,
            CreatingTransactionIdMismatch(
              reassignmentId,
              assignmentRequest.creatingTransactionId,
              transferData.creatingTransactionId,
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
            assignmentRequest.reassignmentCounter == transferData.reassignmentCounter,
            (),
            InconsistentReassignmentCounter(
              reassignmentId,
              assignmentRequest.reassignmentCounter,
              transferData.reassignmentCounter,
            ): TransferProcessorError,
          )

        } yield Some(AssignmentValidationResult(confirmingParties.toSet))
      case None =>
        for {
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          res <-
            if (isReassigningParticipant) {
              // This happens either in case of malicious assignments (incorrectly declared reassigning participants)
              // OR if the transfer data has been pruned.
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
                  TransferProcessorError,
                  Option[AssignmentValidationResult],
                ]
              })
            } else EitherT.rightT[Future, TransferProcessorError](None)
        } yield res
    }
  }
}

object AssignmentValidation {
  final case class AssignmentValidationResult(confirmingParties: Set[LfPartyId])

  private[transfer] sealed trait AssignmentValidationError extends TransferProcessorError

  final case class NoTransferData(
      reassignmentId: ReassignmentId,
      lookupError: TransferStore.TransferLookupError,
  ) extends AssignmentValidationError {
    override def message: String =
      s"Cannot find transfer data for transfer `$reassignmentId`: ${lookupError.cause}"
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

  final case class TransferSigningError(
      cause: SyncCryptoError
  ) extends TransferProcessorError {
    override def message: String = show"Unable to sign transfer request. $cause"
  }
}
