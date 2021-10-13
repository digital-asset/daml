// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.store.DamlStateKey
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueCommitting, Raw}
import com.daml.ledger.validator.reading.StateReader
import com.daml.ledger.validator.{
  HasDamlStateValue,
  HashingLogEntryIdComputationStrategy,
  LogEntryIdComputationStrategy,
  ValidationFailed,
}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}

import scala.collection.compat._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/** Validator for pre-executing submissions.
  *
  * @param committer                     Generates the pre-execution result from the submission.
  * @param commitStrategy                The strategy used to generate the data committed to the ledger.
  * @param logEntryIdComputationStrategy The strategy used to generate the log entry id.
  * @param metrics                       Records metrics.
  * @tparam StateValue The type of the state persisted to the ledger. This must implement
  *                    [[HasDamlStateValue]].
  * @tparam ReadSet    The type of the read set generated by the `commitStrategy`.
  * @tparam WriteSet   The type of the write set generated by the `commitStrategy`.
  */
class PreExecutingSubmissionValidator[StateValue, ReadSet, WriteSet](
    committer: KeyValueCommitting,
    commitStrategy: PreExecutingCommitStrategy[
      DamlStateKey,
      StateValue,
      ReadSet,
      WriteSet,
    ],
    logEntryIdComputationStrategy: LogEntryIdComputationStrategy =
      HashingLogEntryIdComputationStrategy,
    metrics: Metrics,
)(implicit hasDamlStateValue: HasDamlStateValue[StateValue]) {
  private val logger = ContextualizedLogger.get(getClass)

  def validate(
      submissionEnvelope: Raw.Envelope,
      submittingParticipantId: Ref.ParticipantId,
      ledgerStateReader: StateReader[DamlStateKey, StateValue],
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[PreExecutionOutput[ReadSet, WriteSet]] =
    Timed.timedAndTrackedFuture(
      metrics.daml.kvutils.submission.validator.validatePreExecute,
      metrics.daml.kvutils.submission.validator.validatePreExecuteRunning,
      for {
        decodedSubmission <- decodeSubmission(submissionEnvelope)
        fetchedInputs <- fetchSubmissionInputs(decodedSubmission, ledgerStateReader)
        inputState = fetchedInputs.view.mapValues(hasDamlStateValue.damlStateValue).toMap
        preExecutionResult = committer.preExecuteSubmission(
          LedgerReader.DefaultConfiguration,
          decodedSubmission,
          submittingParticipantId,
          inputState,
        )
        logEntryId = logEntryIdComputationStrategy.compute(submissionEnvelope)
        generatedWriteSets <- Timed.future(
          metrics.daml.kvutils.submission.validator.generateWriteSets,
          commitStrategy.generateWriteSets(
            submittingParticipantId,
            logEntryId,
            fetchedInputs,
            preExecutionResult,
          ),
        )
      } yield {
        PreExecutionOutput(
          minRecordTime = preExecutionResult.minimumRecordTime.map(_.toInstant),
          maxRecordTime = preExecutionResult.maximumRecordTime.map(_.toInstant),
          successWriteSet = generatedWriteSets.successWriteSet,
          outOfTimeBoundsWriteSet = generatedWriteSets.outOfTimeBoundsWriteSet,
          readSet = commitStrategy.generateReadSet(fetchedInputs, preExecutionResult.readSet),
          involvedParticipants = generatedWriteSets.involvedParticipants,
        )
      },
    )

  private def decodeSubmission(submissionEnvelope: Raw.Envelope)(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[DamlSubmission] =
    Timed.timedAndTrackedFuture(
      metrics.daml.kvutils.submission.validator.decode,
      metrics.daml.kvutils.submission.validator.decodeRunning,
      Future {
        Envelope.open(submissionEnvelope) match {
          case Right(Envelope.SubmissionMessage(submission)) =>
            metrics.daml.kvutils.submission.validator.receivedSubmissionBytes
              .update(submission.getSerializedSize)
            submission

          case Right(Envelope.SubmissionBatchMessage(_)) =>
            logger.error("Batched submissions are not supported for pre-execution")
            throw ValidationFailed.ValidationError(
              "Batched submissions are not supported for pre-execution"
            )

          case Right(other) =>
            throw ValidationFailed.ValidationError(
              s"Unexpected message in envelope: ${other.getClass.getSimpleName}"
            )

          case Left(error) =>
            throw ValidationFailed.ValidationError(s"Cannot open envelope: $error")
        }
      },
    )

  private def fetchSubmissionInputs(
      submission: DamlSubmission,
      ledgerStateReader: StateReader[DamlStateKey, StateValue],
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Map[DamlStateKey, StateValue]] = {
    val inputKeys = submission.getInputDamlStateList.asScala
    Timed.timedAndTrackedFuture(
      metrics.daml.kvutils.submission.validator.fetchInputs,
      metrics.daml.kvutils.submission.validator.fetchInputsRunning,
      for {
        inputValues <- ledgerStateReader.read(inputKeys)
      } yield {
        assert(inputKeys.size == inputValues.size)
        val inputPairs = inputKeys.iterator zip inputValues.iterator
        inputPairs.toMap
      },
    )
  }
}
