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
import com.digitalasset.canton.participant.protocol.transfer.TransferInValidation.*
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.condUnitET
import com.digitalasset.canton.util.EitherUtil.condUnitE
import com.digitalasset.canton.{LfPartyId, TransferCounter}
import com.digitalasset.daml.lf.engine.Error as LfError
import com.digitalasset.daml.lf.interpretation.Error as LfInterpretationError
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

private[transfer] class TransferInValidation(
    domainId: TargetDomainId,
    staticDomainParameters: StaticDomainParameters,
    participantId: ParticipantId,
    engine: DAMLe,
    transferCoordination: TransferCoordination,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  def checkStakeholders(
      transferInRequest: FullTransferInTree,
      getEngineAbortStatus: GetEngineAbortStatus,
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Unit] = {
    val reassignmentId = transferInRequest.transferOutResultEvent.reassignmentId

    val declaredContractStakeholders = transferInRequest.contract.metadata.stakeholders
    val declaredViewStakeholders = transferInRequest.stakeholders

    for {
      metadata <- engine
        .contractMetadata(
          transferInRequest.contract.contractInstance,
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
  private[transfer] def validateTransferInRequest(
      tsIn: CantonTimestamp,
      transferInRequest: FullTransferInTree,
      transferDataO: Option[TransferData],
      targetCrypto: DomainSnapshotSyncCryptoApi,
      isReassigningParticipant: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Option[TransferInValidationResult]] = {
    val txOutResultEvent = transferInRequest.transferOutResultEvent.result

    val reassignmentId = transferInRequest.transferOutResultEvent.reassignmentId

    def checkSubmitterIsStakeholder: Either[TransferProcessorError, Unit] =
      condUnitE(
        transferInRequest.stakeholders.contains(transferInRequest.submitter),
        SubmittingPartyMustBeStakeholderIn(
          reassignmentId,
          transferInRequest.submitter,
          transferInRequest.stakeholders,
        ),
      )

    val targetIps = targetCrypto.ipsSnapshot

    transferDataO match {
      case Some(transferData) =>
        val sourceDomain = transferData.transferOutRequest.sourceDomain
        val transferOutTimestamp = transferData.transferOutTimestamp
        for {
          _ready <- {
            logger.info(
              s"Waiting for topology state at $transferOutTimestamp on transfer-out domain $sourceDomain ..."
            )
            EitherT(
              transferCoordination
                .awaitTransferOutTimestamp(
                  sourceDomain,
                  staticDomainParameters,
                  transferOutTimestamp,
                )
                .sequence
            )
          }

          sourceCrypto <- transferCoordination.cryptoSnapshot(
            sourceDomain.unwrap,
            staticDomainParameters,
            transferOutTimestamp,
          )
          // TODO(i12926): Check the signatures of the mediator and the sequencer

          _ <- condUnitET[Future](
            txOutResultEvent.content.timestamp <= transferData.transferOutDecisionTime,
            ResultTimestampExceedsDecisionTime(
              reassignmentId,
              timestamp = txOutResultEvent.content.timestamp,
              decisionTime = transferData.transferOutDecisionTime,
            ),
          )

          // TODO(i12926): Validate the shipped transfer-out result w.r.t. stakeholders
          // TODO(i12926): Validate that the transfer-out result received matches the transfer-out result in transferData

          _ <- condUnitET[Future](
            transferInRequest.contract == transferData.contract,
            ContractDataMismatch(reassignmentId),
          )
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          transferOutSubmitter = transferData.transferOutRequest.submitter
          targetTimeProof = transferData.transferOutRequest.targetTimeProof.timestamp

          // TODO(i12926): Check that transferData.transferOutRequest.targetTimeProof.timestamp is in the past
          cryptoSnapshot <- transferCoordination
            .cryptoSnapshot(
              transferData.targetDomain.unwrap,
              staticDomainParameters,
              targetTimeProof,
            )

          exclusivityLimit <- ProcessingSteps
            .getTransferInExclusivity(
              cryptoSnapshot.ipsSnapshot,
              targetTimeProof,
            )
            .leftMap[TransferProcessorError](TransferParametersError(domainId.unwrap, _))

          _ <- condUnitET[Future](
            tsIn >= exclusivityLimit
              || transferOutSubmitter == transferInRequest.submitter,
            NonInitiatorSubmitsBeforeExclusivityTimeout(
              reassignmentId,
              transferInRequest.submitter,
              currentTimestamp = tsIn,
              timeout = exclusivityLimit,
            ),
          )
          _ <- condUnitET[Future](
            transferData.creatingTransactionId == transferInRequest.creatingTransactionId,
            CreatingTransactionIdMismatch(
              reassignmentId,
              transferInRequest.creatingTransactionId,
              transferData.creatingTransactionId,
            ),
          )
          sourceIps = sourceCrypto.ipsSnapshot

          stakeholders = transferInRequest.stakeholders
          sourceConfirmingParties <- EitherT.right(
            sourceIps.canConfirm(participantId, stakeholders)
          )
          targetConfirmingParties <- EitherT.right(
            targetIps.canConfirm(participantId, stakeholders)
          )
          confirmingParties = sourceConfirmingParties.intersect(targetConfirmingParties)

          _ <- EitherT.cond[Future](
            // transfer counter is the same in transfer-out and transfer-in requests
            transferInRequest.transferCounter == transferData.transferCounter,
            (),
            InconsistentTransferCounter(
              reassignmentId,
              transferInRequest.transferCounter,
              transferData.transferCounter,
            ): TransferProcessorError,
          )

        } yield Some(TransferInValidationResult(confirmingParties.toSet))
      case None =>
        for {
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          res <-
            if (isReassigningParticipant) {
              // This happens either in case of malicious transfer-ins (incorrectly declared reassigning participants)
              // OR if the transfer data has been pruned.
              // The transfer-in should be rejected due to other validations (e.g. conflict detection), but
              // we could code this more defensively at some point

              val targetIps = targetCrypto.ipsSnapshot
              val confirmingPartiesF = targetIps
                .canConfirm(
                  participantId,
                  transferInRequest.stakeholders,
                )
              EitherT(confirmingPartiesF.map { confirmingParties =>
                Right(Some(TransferInValidationResult(confirmingParties))): Either[
                  TransferProcessorError,
                  Option[TransferInValidationResult],
                ]
              })
            } else EitherT.rightT[Future, TransferProcessorError](None)
        } yield res
    }
  }
}

object TransferInValidation {
  final case class TransferInValidationResult(confirmingParties: Set[LfPartyId])

  private[transfer] sealed trait TransferInValidationError extends TransferProcessorError

  final case class NoTransferData(
      reassignmentId: ReassignmentId,
      lookupError: TransferStore.TransferLookupError,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot find transfer data for transfer `$reassignmentId`: ${lookupError.cause}"
  }

  final case class TransferOutIncomplete(reassignmentId: ReassignmentId, participant: ParticipantId)
      extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$reassignmentId` because transfer-out is incomplete"
  }

  final case class NoParticipantForReceivingParty(reassignmentId: ReassignmentId, party: LfPartyId)
      extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$reassignmentId` because $party is not active"
  }

  final case class UnexpectedDomain(
      reassignmentId: ReassignmentId,
      targetDomain: DomainId,
      receivedOn: DomainId,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$reassignmentId`: expecting domain `$targetDomain` but received on `$receivedOn`"
  }

  final case class ResultTimestampExceedsDecisionTime(
      reassignmentId: ReassignmentId,
      timestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$reassignmentId`: result time $timestamp exceeds decision time $decisionTime"
  }

  final case class NonInitiatorSubmitsBeforeExclusivityTimeout(
      reassignmentId: ReassignmentId,
      submitter: LfPartyId,
      currentTimestamp: CantonTimestamp,
      timeout: CantonTimestamp,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$reassignmentId`: only submitter can initiate before exclusivity timeout $timeout"
  }

  final case class ContractDataMismatch(reassignmentId: ReassignmentId)
      extends TransferInValidationError {
    override def message: String = s"Cannot transfer-in `$reassignmentId`: contract data mismatch"
  }

  final case class CreatingTransactionIdMismatch(
      reassignmentId: ReassignmentId,
      transferInTransactionId: TransactionId,
      localTransactionId: TransactionId,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$reassignmentId`: creating transaction id mismatch"
  }

  final case class InconsistentTransferCounter(
      reassignmentId: ReassignmentId,
      declaredTransferCounter: TransferCounter,
      expectedTransferCounter: TransferCounter,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in $reassignmentId: Transfer counter $declaredTransferCounter in transfer-in does not match $expectedTransferCounter from the transfer-out"
  }

  final case class TransferSigningError(
      cause: SyncCryptoError
  ) extends TransferProcessorError {
    override def message: String = show"Unable to sign transfer request. $cause"
  }
}
