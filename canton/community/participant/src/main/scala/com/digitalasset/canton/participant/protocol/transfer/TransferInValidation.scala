// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.lf.engine.Error as LfError
import com.daml.lf.interpretation.Error as LfInterpretationError
import com.digitalasset.canton.crypto.DomainSnapshotSyncCryptoApi
import com.digitalasset.canton.data.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
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
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{LfPartyId, TransferCounterO}
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}

private[transfer] class TransferInValidation(
    domainId: TargetDomainId,
    participantId: ParticipantId,
    engine: DAMLe,
    transferCoordination: TransferCoordination,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  def checkStakeholders(
      transferInRequest: FullTransferInTree
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Unit] = {
    val transferId = transferInRequest.transferOutResultEvent.transferId

    val declaredContractStakeholders = transferInRequest.contract.metadata.stakeholders
    val declaredViewStakeholders = transferInRequest.stakeholders

    for {
      metadata <- engine
        .contractMetadata(
          transferInRequest.contract.contractInstance,
          declaredContractStakeholders,
        )
        .leftMap {
          case LfError.Interpretation(
                e @ LfError.Interpretation.DamlException(
                  LfInterpretationError.FailedAuthorization(_, _)
                ),
                _,
              ) =>
            StakeholdersMismatch(
              Some(transferId),
              declaredViewStakeholders = declaredViewStakeholders,
              declaredContractStakeholders = Some(declaredContractStakeholders),
              expectedStakeholders = Left(e.message),
            )
          case error => MetadataNotFound(error)
        }
      recomputedStakeholders = metadata.stakeholders
      _ <- condUnitET[Future](
        declaredViewStakeholders == recomputedStakeholders && declaredViewStakeholders == declaredContractStakeholders,
        StakeholdersMismatch(
          Some(transferId),
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
      transferringParticipant: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Option[TransferInValidationResult]] = {
    val txOutResultEvent = transferInRequest.transferOutResultEvent.result

    val transferId = transferInRequest.transferOutResultEvent.transferId

    def checkSubmitterIsStakeholder: Either[TransferProcessorError, Unit] =
      condUnitE(
        transferInRequest.stakeholders.contains(transferInRequest.submitter),
        SubmittingPartyMustBeStakeholderIn(
          transferId,
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
              s"Waiting for topology state at ${transferOutTimestamp} on transfer-out domain $sourceDomain ..."
            )
            EitherT(
              transferCoordination
                .awaitTransferOutTimestamp(sourceDomain, transferOutTimestamp)
                .sequence
            )
          }

          sourceCrypto <- transferCoordination.cryptoSnapshot(
            sourceDomain.unwrap,
            transferOutTimestamp,
          )
          // TODO(i12926): Check the signatures of the mediator and the sequencer

          _ <- condUnitET[Future](
            txOutResultEvent.content.timestamp <= transferData.transferOutDecisionTime,
            ResultTimestampExceedsDecisionTime(
              transferId,
              timestamp = txOutResultEvent.content.timestamp,
              decisionTime = transferData.transferOutDecisionTime,
            ),
          )

          // TODO(i12926): Validate the shipped transfer-out result w.r.t. stakeholders
          // TODO(i12926): Validate that the transfer-out result received matches the transfer-out result in transferData

          _ <- condUnitET[Future](
            transferInRequest.contract == transferData.contract,
            ContractDataMismatch(transferId),
          )
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          transferOutSubmitter = transferData.transferOutRequest.submitter
          targetTimeProof = transferData.transferOutRequest.targetTimeProof.timestamp

          // TODO(i12926): Check that transferData.transferOutRequest.targetTimeProof.timestamp is in the past
          cryptoSnapshot <- transferCoordination
            .cryptoSnapshot(transferData.targetDomain.unwrap, targetTimeProof)

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
              transferId,
              transferInRequest.submitter,
              currentTimestamp = tsIn,
              timeout = exclusivityLimit,
            ),
          )
          _ <- condUnitET[Future](
            transferData.creatingTransactionId == transferInRequest.creatingTransactionId,
            CreatingTransactionIdMismatch(
              transferId,
              transferInRequest.creatingTransactionId,
              transferData.creatingTransactionId,
            ),
          )
          sourceIps = sourceCrypto.ipsSnapshot
          confirmingParties <- EitherT.right(
            transferInRequest.stakeholders.toList.parTraverseFilter { stakeholder =>
              for {
                source <- sourceIps.canConfirm(participantId, stakeholder)
                target <- targetIps.canConfirm(participantId, stakeholder)
              } yield if (source && target) Some(stakeholder) else None
            }
          )

          _ <- EitherT.cond[Future](
            // transfer counter is the same in transfer-out and transfer-in requests
            transferInRequest.transferCounter == transferData.transferCounter,
            (),
            InconsistentTransferCounter(
              transferId,
              transferInRequest.transferCounter,
              transferData.transferCounter,
            ): TransferProcessorError,
          )

        } yield Some(TransferInValidationResult(confirmingParties.toSet))
      case None =>
        for {
          _ <- EitherT.fromEither[Future](checkSubmitterIsStakeholder)
          res <-
            if (transferringParticipant) {
              val targetIps = targetCrypto.ipsSnapshot
              val confirmingPartiesF = transferInRequest.stakeholders.toList
                .parTraverseFilter { stakeholder =>
                  targetIps
                    .canConfirm(participantId, stakeholder)
                    .map(if (_) Some(stakeholder) else None)
                }
                .map(_.toSet)
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
      transferId: TransferId,
      lookupError: TransferStore.TransferLookupError,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot find transfer data for transfer `$transferId`: ${lookupError.cause}"
  }

  final case class TransferOutIncomplete(transferId: TransferId, participant: ParticipantId)
      extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId` because transfer-out is incomplete"
  }

  final case class NoParticipantForReceivingParty(transferId: TransferId, party: LfPartyId)
      extends TransferInValidationError {
    override def message: String = s"Cannot transfer-in `$transferId` because $party is not active"
  }

  final case class UnexpectedDomain(
      transferId: TransferId,
      targetDomain: DomainId,
      receivedOn: DomainId,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: expecting domain `$targetDomain` but received on `$receivedOn`"
  }

  final case class ResultTimestampExceedsDecisionTime(
      transferId: TransferId,
      timestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: result time $timestamp exceeds decision time $decisionTime"
  }

  final case class NonInitiatorSubmitsBeforeExclusivityTimeout(
      transferId: TransferId,
      submitter: LfPartyId,
      currentTimestamp: CantonTimestamp,
      timeout: CantonTimestamp,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: only submitter can initiate before exclusivity timeout $timeout"
  }

  final case class ContractDataMismatch(transferId: TransferId) extends TransferInValidationError {
    override def message: String = s"Cannot transfer-in `$transferId`: contract data mismatch"
  }

  final case class CreatingTransactionIdMismatch(
      transferId: TransferId,
      transferInTransactionId: TransactionId,
      localTransactionId: TransactionId,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in `$transferId`: creating transaction id mismatch"
  }

  final case class InconsistentTransferCounter(
      transferId: TransferId,
      declaredTransferCounter: TransferCounterO,
      expectedTransferCounter: TransferCounterO,
  ) extends TransferInValidationError {
    override def message: String =
      s"Cannot transfer-in $transferId: Transfer counter $declaredTransferCounter in transfer-in does not match $expectedTransferCounter from the transfer-out"
  }

}
