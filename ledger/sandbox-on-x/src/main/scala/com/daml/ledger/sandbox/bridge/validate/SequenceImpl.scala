// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.api.util.TimeProvider
import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.v2.{ChangeId, CompletionInfo, Update}
import com.daml.ledger.sandbox.bridge.LedgerBridge.{fromOffset, toOffset, _}
import com.daml.ledger.sandbox.bridge._
import com.daml.ledger.sandbox.bridge.validate.ConflictCheckingLedgerBridge.{
  Sequence,
  Validation,
  _,
}
import com.daml.ledger.sandbox.bridge.validate.SequencerState.LastUpdatedAt
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission.{AllocateParty, Config, Transaction}
import com.daml.ledger.sandbox.domain._
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.data.Ref.SubmissionId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{Transaction => LfTransaction}
import com.daml.logging.ContextualizedLogger
import com.daml.metrics.Timed
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.events._

import java.time.Duration
import scala.util.chaining._

/** Conflict checking with the in-flight commands,
  * assigns offsets and converts the accepted/rejected commands to updates.
  */
private[validate] class SequenceImpl(
    participantId: Ref.ParticipantId,
    timeProvider: TimeProvider,
    initialLedgerEnd: Offset,
    initialAllocatedParties: Set[Ref.Party],
    initialLedgerConfiguration: Option[Configuration],
    validatePartyAllocation: Boolean,
    bridgeMetrics: BridgeMetrics,
    errorFactories: ErrorFactories,
    maxDeduplicationDuration: Duration,
) extends Sequence {
  private[this] implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  @volatile private[validate] var offsetIdx = fromOffset(initialLedgerEnd)
  @volatile private[validate] var sequencerState = SequencerState.empty(bridgeMetrics)
  @volatile private[validate] var allocatedParties = initialAllocatedParties
  @volatile private[validate] var ledgerConfiguration = initialLedgerConfiguration
  @volatile private[validate] var deduplicationState =
    DeduplicationState.empty(maxDeduplicationDuration, bridgeMetrics)

  override def apply(): Validation[(Offset, PreparedSubmission)] => Iterable[(Offset, Update)] =
    in => {
      Timed.value(
        bridgeMetrics.Stages.sequence, {
          offsetIdx = offsetIdx + 1L
          val newOffset = toOffset(offsetIdx)
          val recordTime = timeProvider.getCurrentTimestamp

          val updateO = in match {
            case Left(rejection) => Some(rejection.toCommandRejectedUpdate(recordTime))
            case Right((_, NoOpPreparedSubmission(submission))) =>
              processNonTransactionSubmission(submission)
            case Right((noConflictUpTo, txSubmission: PreparedTransactionSubmission)) =>
              Some(
                sequentialTransactionValidation(
                  noConflictUpTo,
                  newOffset,
                  recordTime,
                  txSubmission,
                )
              )
          }

          updateO.map(newOffset -> _).toList
        },
      )
    }

  private val processNonTransactionSubmission: Submission => Option[Update] = {
    case s: Submission.AllocateParty => validatedPartyAllocation(s)
    case s @ Submission.Config(maxRecordTime, submissionId, config) =>
      Some(validatedConfigUpload(s, maxRecordTime, submissionId, config))
    case s: Submission.UploadPackages =>
      Some(packageUploadSuccess(s, timeProvider.getCurrentTimestamp))
    case _: Submission.Transaction =>
      throw new RuntimeException("Unexpected Submission.Transaction")
  }

  private def validatedPartyAllocation(allocateParty: AllocateParty) = {
    val partyAllocation =
      partyAllocationSuccess(allocateParty, participantId, timeProvider.getCurrentTimestamp)

    val party = partyAllocation.party

    if (allocatedParties(party)) {
      logger.warn(
        s"Found duplicate party '$party' for submissionId ${allocateParty.submissionId}"
      )(allocateParty.loggingContext)
      // Duplicate party allocations are skipped
      None
    } else {
      allocatedParties = allocatedParties + party
      Some(partyAllocation)
    }
  }

  private def validatedConfigUpload(
      c: Config,
      maxRecordTime: Timestamp,
      submissionId: SubmissionId,
      config: Configuration,
  ) = {
    val recordTime = timeProvider.getCurrentTimestamp
    if (recordTime > maxRecordTime)
      Update.ConfigurationChangeRejected(
        recordTime = recordTime,
        submissionId = submissionId,
        participantId = participantId,
        proposedConfiguration = config,
        rejectionReason = s"Configuration change timed out: $recordTime > $maxRecordTime",
      )
    else {
      val expectedGeneration = ledgerConfiguration.map(_.generation).map(_ + 1L)
      if (expectedGeneration.forall(_ == config.generation)) {
        ledgerConfiguration = Some(config)
        configChangedSuccess(c, participantId, timeProvider.getCurrentTimestamp)
      } else
        Update.ConfigurationChangeRejected(
          recordTime = recordTime,
          submissionId = submissionId,
          participantId = participantId,
          proposedConfiguration = config,
          rejectionReason =
            s"Generation mismatch: expected=$expectedGeneration, actual=${config.generation}",
        )
    }
  }

  private def sequentialTransactionValidation(
      noConflictUpTo: Offset,
      newOffset: LastUpdatedAt,
      recordTime: Timestamp,
      txSubmission: PreparedTransactionSubmission,
  ): Update =
    withErrorLogger(txSubmission.submission.submitterInfo.submissionId) { implicit errorLogger =>
      val submitterInfo = txSubmission.submission.submitterInfo
      val completionInfo = submitterInfo.toCompletionInfo()

      for {
        _ <- checkTimeModel(
          transaction = txSubmission.submission,
          recordTime = recordTime,
          ledgerConfiguration = ledgerConfiguration,
        )
        _ <- validateParties(
          allocatedParties = allocatedParties,
          transactionInformees = txSubmission.transactionInformees,
          completionInfo = completionInfo,
        )
        _ <- conflictCheckWithInFlight(
          keysState = sequencerState.keyState,
          consumedContractsState = sequencerState.consumedContractsState,
          keyInputs = txSubmission.keyInputs,
          inputContracts = txSubmission.inputContracts,
          completionInfo = completionInfo,
        )
        recordTime = timeProvider.getCurrentTimestamp
        updatedDeduplicationState <- deduplicate(
          changeId = ChangeId(
            submitterInfo.applicationId,
            submitterInfo.commandId,
            submitterInfo.actAs.toSet,
          ),
          deduplicationPeriod = submitterInfo.deduplicationPeriod,
          completionInfo = completionInfo,
          recordTime = recordTime,
        )
        _ = updateStatesOnSuccessfulValidation(
          noConflictUpTo,
          newOffset,
          txSubmission,
          updatedDeduplicationState,
        )
      } yield transactionAccepted(
        txSubmission.submission,
        offsetIdx,
        recordTime,
      )
    }(txSubmission.submission.loggingContext, logger)
      .fold(_.toCommandRejectedUpdate(recordTime), identity)

  private def conflictCheckWithInFlight(
      keysState: Map[Key, (Option[ContractId], LastUpdatedAt)],
      consumedContractsState: Set[ContractId],
      keyInputs: KeyInputs,
      inputContracts: Set[ContractId],
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Validation[Unit] =
    inputContracts.intersect(consumedContractsState) pipe {
      case alreadyArchived if alreadyArchived.nonEmpty =>
        Left(UnknownContracts(alreadyArchived)(completionInfo, errorFactories))
      case _ =>
        keyInputs
          .foldLeft[Validation[Unit]](Right(())) {
            case (Right(_), (key, LfTransaction.KeyCreate)) =>
              keysState.get(key) match {
                case None | Some((None, _)) => Right(())
                case Some((Some(_), _)) => Left(DuplicateKey(key)(completionInfo, errorFactories))
              }
            case (Right(_), (key, LfTransaction.NegativeKeyLookup)) =>
              keysState.get(key) match {
                case None | Some((None, _)) => Right(())
                case Some((Some(actual), _)) =>
                  Left(InconsistentContractKey(None, Some(actual))(completionInfo, errorFactories))
              }
            case (Right(_), (key, LfTransaction.KeyActive(cid))) =>
              keysState.get(key) match {
                case None | Some((Some(`cid`), _)) => Right(())
                case Some((other, _)) =>
                  Left(InconsistentContractKey(other, Some(cid))(completionInfo, errorFactories))
              }
            case (left, _) => left
          }
    }

  private def deduplicate(
      changeId: ChangeId,
      deduplicationPeriod: DeduplicationPeriod,
      completionInfo: CompletionInfo,
      recordTime: Time.Timestamp,
  )(implicit
      errorLogger: ContextualizedErrorLogger
  ): Validation[DeduplicationState] =
    deduplicationPeriod match {
      case DeduplicationPeriod.DeduplicationDuration(commandDeduplicationDuration) =>
        val (newDeduplicationState, isDuplicate) =
          deduplicationState.deduplicate(changeId, commandDeduplicationDuration, recordTime)

        Either.cond(
          !isDuplicate,
          newDeduplicationState,
          DuplicateCommand(changeId, completionInfo),
        )
      case _: DeduplicationPeriod.DeduplicationOffset =>
        Left(Rejection.OffsetDeduplicationPeriodUnsupported(completionInfo))
    }

  private def validateParties(
      allocatedParties: Set[Ref.Party],
      transactionInformees: Set[Ref.Party],
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Validation[Unit] =
    if (validatePartyAllocation) {
      val unallocatedInformees = transactionInformees diff allocatedParties
      Either.cond(
        unallocatedInformees.isEmpty,
        (),
        UnallocatedParties(unallocatedInformees.toSet)(completionInfo, errorFactories),
      )
    } else Right(())

  private def checkTimeModel(
      transaction: Transaction,
      recordTime: Timestamp,
      ledgerConfiguration: Option[Configuration],
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): Validation[Unit] = {
    val completionInfo = transaction.submitterInfo.toCompletionInfo()
    ledgerConfiguration
      .toRight(Rejection.NoLedgerConfiguration(completionInfo, errorFactories))
      .flatMap(configuration =>
        configuration.timeModel
          .checkTime(
            transaction.transactionMeta.ledgerEffectiveTime,
            recordTime,
          )
          .left
          .map(Rejection.InvalidLedgerTime(completionInfo, _)(errorFactories))
      )
  }

  private def updateStatesOnSuccessfulValidation(
      noConflictUpTo: LastUpdatedAt,
      newOffset: LastUpdatedAt,
      txSubmission: PreparedTransactionSubmission,
      updatedDeduplicationState: DeduplicationState,
  ): Unit = {
    sequencerState = sequencerState
      .dequeue(noConflictUpTo)
      .enqueue(
        newOffset,
        txSubmission.updatedKeys,
        txSubmission.consumedContracts,
      )
    deduplicationState = updatedDeduplicationState
  }
}
