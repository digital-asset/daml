// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlSubmission}
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.{
  Bytes,
  DamlStateMapWithFingerprints,
  Envelope,
  KeyValueCommitting
}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.batch.BatchedSubmissionValidator
import com.daml.ledger.validator.preexecution.PreExecutingSubmissionValidator._
import com.daml.ledger.validator.preexecution.PreExecutionCommitResult.ReadSet
import com.daml.ledger.validator.{StateKeySerializationStrategy, ValidationFailed}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Validator pre-executing submissions.
  */
class PreExecutingSubmissionValidator[WriteSet](
    committer: KeyValueCommitting,
    metrics: Metrics,
    keySerializationStrategy: StateKeySerializationStrategy,
    commitStrategy: PreExecutingCommitStrategy[WriteSet]) {
  private val logger = ContextualizedLogger.get(getClass)

  def validate(
      submissionEnvelope: Bytes,
      submittingParticipantId: ParticipantId,
      ledgerStateReader: DamlLedgerStateReaderWithFingerprints,
  )(
      implicit executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[PreExecutionOutput[WriteSet]] =
    Timed.timedAndTrackedFuture(
      metrics.daml.kvutils.submission.validator.validatePreExecute,
      metrics.daml.kvutils.submission.validator.validatePreExecuteRunning,
      for {
        decodedSubmission <- decodeSubmission(submissionEnvelope)
        fetchedInputs <- fetchSubmissionInputs(decodedSubmission, ledgerStateReader)
        preExecutionResult <- preExecuteSubmission(
          decodedSubmission,
          submittingParticipantId,
          fetchedInputs)
        logEntryId = BatchedSubmissionValidator.bytesToLogEntryId(submissionEnvelope)
        inputState = fetchedInputs.map { case (key, (value, _)) => key -> value }
        generatedWriteSets <- Timed.future(
          metrics.daml.kvutils.submission.validator.generateWriteSets,
          commitStrategy
            .generateWriteSets(submittingParticipantId, logEntryId, inputState, preExecutionResult)
        )
      } yield {
        PreExecutionOutput(
          minRecordTime = preExecutionResult.minimumRecordTime.map(_.toInstant),
          maxRecordTime = preExecutionResult.maximumRecordTime.map(_.toInstant),
          successWriteSet = generatedWriteSets.successWriteSet,
          outOfTimeBoundsWriteSet = generatedWriteSets.outOfTimeBoundsWriteSet,
          readSet = generateReadSet(fetchedInputs, preExecutionResult.readSet),
          involvedParticipants = generatedWriteSets.involvedParticipants
        )
      }
    )

  private def decodeSubmission(submissionEnvelope: Bytes)(
      implicit executionContext: ExecutionContext,
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
              "Batched submissions are not supported for pre-execution")

          case Right(other) =>
            throw ValidationFailed.ValidationError(
              s"Unexpected message in envelope: ${other.getClass.getSimpleName}")

          case Left(error) =>
            throw ValidationFailed.ValidationError(s"Cannot open envelope: $error")
        }
      }
    )

  private def fetchSubmissionInputs(
      submission: DamlSubmission,
      ledgerStateReader: DamlLedgerStateReaderWithFingerprints)(
      implicit executionContext: ExecutionContext): Future[DamlStateMapWithFingerprints] = {
    val inputKeys = submission.getInputDamlStateList.asScala
    Timed.timedAndTrackedFuture(
      metrics.daml.kvutils.submission.validator.fetchInputs,
      metrics.daml.kvutils.submission.validator.fetchInputsRunning,
      for {
        inputValues <- ledgerStateReader.read(inputKeys)

        nestedInputKeys = inputValues.collect {
          case (Some(value), _) if value.hasContractKeyState =>
            val contractId = value.getContractKeyState.getContractId
            DamlStateKey.newBuilder.setContractId(contractId).build
        }
        nestedInputValues <- ledgerStateReader.read(nestedInputKeys)
      } yield {
        assert(inputKeys.size == inputValues.size)
        assert(nestedInputKeys.size == nestedInputValues.size)
        val inputPairs = inputKeys.toIterator zip inputValues.toIterator
        val nestedInputPairs = nestedInputKeys.toIterator zip nestedInputValues.toIterator
        (inputPairs ++ nestedInputPairs).toMap
      }
    )
  }

  private def preExecuteSubmission(
      submission: DamlSubmission,
      submittingParticipantId: ParticipantId,
      inputState: DamlStateMapWithFingerprints)(
      implicit executionContext: ExecutionContext): Future[PreExecutionResult] = Future {
    committer.preExecuteSubmission(
      LedgerReader.DefaultConfiguration,
      submission,
      submittingParticipantId,
      inputState.mapValues { case (value, _) => value })
  }

  private[preexecution] def generateReadSet(
      fetchedInputs: DamlStateMapWithFingerprints,
      accessedKeys: Set[DamlStateKey],
  ): ReadSet =
    accessedKeys
      .map { key =>
        val (_, fingerprint) =
          fetchedInputs.getOrElse(key, throw new KeyNotPresentInInputException(key))
        key -> fingerprint
      }
      .map {
        case (damlKey, fingerprint) =>
          keySerializationStrategy.serializeStateKey(damlKey) -> fingerprint
      }
      .toVector
      .sortBy(_._1.asReadOnlyByteBuffer)
}

object PreExecutingSubmissionValidator {

  final class KeyNotPresentInInputException(key: DamlStateKey)
      extends IllegalStateException(
        s"The committer accessed a key that was not present in the input.\nKey: $key")

}
