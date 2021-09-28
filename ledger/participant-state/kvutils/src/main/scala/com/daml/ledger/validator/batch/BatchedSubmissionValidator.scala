// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.wire._
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.export.{
  LedgerDataExporter,
  SubmissionAggregator,
  SubmissionInfo,
}
import com.daml.ledger.participant.state.kvutils.{CorrelationId, Envelope, KeyValueCommitting, Raw}
import com.daml.ledger.validator
import com.daml.ledger.validator.SubmissionValidator.LogEntryAndState
import com.daml.ledger.validator._
import com.daml.ledger.validator.reading.DamlLedgerStateReader
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Ref, Time}
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

object BatchedSubmissionValidator {
  def apply[CommitResult](
      params: BatchedSubmissionValidatorParameters,
      committer: KeyValueCommitting,
      conflictDetection: ConflictDetection,
      metrics: Metrics,
      ledgerDataExporter: LedgerDataExporter,
      logEntryIdComputationStrategy: LogEntryIdComputationStrategy =
        HashingLogEntryIdComputationStrategy,
  ): BatchedSubmissionValidator[CommitResult] =
    new BatchedSubmissionValidator[CommitResult](
      params,
      committer,
      conflictDetection,
      metrics,
      ledgerDataExporter,
      logEntryIdComputationStrategy,
    )

  /** A [[DamlSubmission]] with an associated correlation id and a log entry id computed
    * from the envelope.
    */
  private case class CorrelatedSubmission(
      correlationId: CorrelationId,
      logEntryId: DamlLogEntryId,
      submission: DamlSubmission,
  )

  private def withCorrelationIdLogged[T](
      correlationId: CorrelationId
  )(f: LoggingContext => T): T = {
    newLoggingContextWith("correlationId" -> correlationId) { loggingContext =>
      f(loggingContext)
    }
  }

  private def withSubmissionLoggingContext[T](correlatedSubmission: CorrelatedSubmission)(
      f: LoggingContext => T
  ): T =
    withCorrelationIdLogged(correlatedSubmission.correlationId)(f)
}

/** Batch validator validates and commits Daml submission batches to a Daml ledger. */
class BatchedSubmissionValidator[CommitResult] private[validator] (
    params: BatchedSubmissionValidatorParameters,
    committer: KeyValueCommitting,
    conflictDetection: ConflictDetection,
    damlMetrics: Metrics,
    ledgerDataExporter: LedgerDataExporter,
    logEntryIdComputationStrategy: LogEntryIdComputationStrategy,
) {

  import BatchedSubmissionValidator._

  private val logger = ContextualizedLogger.get(getClass)
  private val metrics = damlMetrics.daml.kvutils.submission.validator

  /** Validate and commit a submission to the ledger.
    *
    * On errors the future is completed with [[com.daml.ledger.validator.ValidationFailed]]
    * and all unprocessed submissions are discarded. Note that some submissions may have already
    * been committed. It is up to the caller to discard, or not to, a partially successful batch.
    */
  def validateAndCommit(
      submissionEnvelope: Raw.Envelope,
      correlationId: CorrelationId,
      recordTimeInstant: Instant,
      participantId: Ref.ParticipantId,
      ledgerStateReader: DamlLedgerStateReader,
      commitStrategy: CommitStrategy[CommitResult],
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] =
    withCorrelationIdLogged(correlationId) { implicit loggingContext =>
      val recordTime = Time.Timestamp.assertFromInstant(recordTimeInstant)
      val submissionInfo =
        SubmissionInfo(participantId, correlationId, submissionEnvelope, recordTimeInstant)
      val exporterAggregator = ledgerDataExporter.addSubmission(submissionInfo)
      Timed.future(
        metrics.validateAndCommit, {
          val result = metrics.openEnvelope.time(() => Envelope.open(submissionEnvelope)) match {
            case Right(Envelope.SubmissionMessage(submission)) =>
              processBatch(
                participantId,
                recordTime,
                singleSubmissionSource(submissionEnvelope, submission, correlationId),
                ledgerStateReader,
                commitStrategy,
                exporterAggregator,
              )

            case Right(Envelope.SubmissionBatchMessage(batch)) =>
              logger.trace(s"Validating a batch of ${batch.getSubmissionsCount} submissions")
              metrics.batchSizes.update(batch.getSubmissionsCount)
              metrics.receivedBatchSubmissionBytes.update(batch.getSerializedSize)
              processBatch(
                participantId,
                recordTime,
                batchSubmissionSource(batch),
                ledgerStateReader,
                commitStrategy,
                exporterAggregator,
              )

            case Right(other) =>
              Future.failed(
                ValidationFailed.ValidationError(
                  s"Unexpected message in envelope: ${other.getClass.getSimpleName}"
                )
              )

            case Left(error) =>
              Future.failed(ValidationFailed.ValidationError(s"Cannot open envelope: $error"))
          }

          result
            .andThen {
              case Failure(exception) =>
                logger.error(s"Validation failure: $exception")
              case Success(_) =>
                ()
            }
        },
      )
    }

  private def singleSubmissionSource(
      envelope: Raw.Envelope,
      submission: DamlSubmission,
      correlationId: CorrelationId,
  ): Source[Inputs, NotUsed] = {
    val logEntryId = logEntryIdComputationStrategy.compute(envelope)
    Source.single(Indexed(CorrelatedSubmission(correlationId, logEntryId, submission), 0L))
  }

  private def batchSubmissionSource(
      batch: DamlSubmissionBatch
  )(implicit executionContext: ExecutionContext): Source[Inputs, NotUsed] =
    Source(
      Indexed
        .fromSeq(
          batch.getSubmissionsList.asScala
            .map(cs => cs.getCorrelationId -> cs.getSubmission)
        )
    )
      .mapAsyncUnordered(params.cpuParallelism) {
        _.mapFuture { case (correlationId, submissionEnvelope) =>
          // Decompress and decode the submissions in parallel.
          Timed.timedAndTrackedFuture(
            metrics.decode,
            metrics.decodeRunning,
            Future {
              val rawEnvelope = Raw.Envelope(submissionEnvelope)
              val logEntryId = logEntryIdComputationStrategy.compute(rawEnvelope)
              val submission = Envelope
                .openSubmission(rawEnvelope)
                .fold(error => throw validator.ValidationFailed.ValidationError(error), identity)
              metrics.receivedSubmissionBytes.update(submission.getSerializedSize)
              CorrelatedSubmission(
                correlationId,
                logEntryId,
                submission,
              )
            },
          )
        }
      }

  private type DamlInputState = Map[DamlStateKey, Option[DamlStateValue]]

  //
  // The following type aliases describe how the batch processing pipeline transforms the data:
  //

  // The batch pipeline starts with a stream of correlated submissions that carry their original index
  // in the batch.
  private type Inputs = Indexed[CorrelatedSubmission]

  // The second stage resolves the inputs to each submission.
  private type FetchedInput = (CorrelatedSubmission, DamlInputState)
  private type Outputs1 = Indexed[FetchedInput]

  // Third stage validates the submission, adding in the validation results.
  private case class ValidatedSubmission(
      correlatedSubmission: CorrelatedSubmission,
      inputState: DamlInputState,
      logEntryAndState: LogEntryAndState,
      exporterWriteSet: SubmissionAggregator.WriteSetBuilder,
  )

  private type Outputs2 = Indexed[ValidatedSubmission]

  // Fourth stage collects the results.
  private type Outputs3 = List[Outputs2]

  // The fifth stage sorts the results and drops the index.
  private type Outputs4 = ValidatedSubmission

  // Sixth stage performs conflict detection and potentially drops conflicting results.
  private type Outputs5 = Outputs4

  // The last stage commits the results.
  private type Outputs6 = Unit

  /** Validate and commit a batch of indexed Daml submissions.
    * See the type definitions above to understand the different stages in the
    * processing pipeline.
    */
  private def processBatch(
      participantId: Ref.ParticipantId,
      recordTime: Timestamp,
      indexedSubmissions: Source[Inputs, NotUsed],
      damlLedgerStateReader: DamlLedgerStateReader,
      commitStrategy: CommitStrategy[CommitResult],
      exporterAggregator: SubmissionAggregator,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Unit] =
    indexedSubmissions
      // Fetch the submission inputs in parallel.
      .mapAsyncUnordered[Outputs1](params.readParallelism) {
        _.mapFuture(fetchSubmissionInputs(_, damlLedgerStateReader))
      }
      // Validate the submissions in parallel.
      .mapAsyncUnordered[Outputs2](params.cpuParallelism) {
        _.mapFuture { case (correlatedSubmission, inputState) =>
          val exporterWriteSet = exporterAggregator.addChild()
          validateSubmission(
            participantId,
            recordTime,
            correlatedSubmission,
            inputState,
            exporterWriteSet,
          )
        }
      }
      // Collect the results.
      .fold(List.empty[Outputs2]) { case (results: Outputs3, result: Outputs2) =>
        result :: results
      }
      // Sort the results and drop the index.
      .mapConcat[Outputs4] { results: Outputs3 =>
        results.sortBy(_.index).map(_.value)
      }
      // Conflict detect and either recover or drop the result.
      .statefulMapConcat[Outputs5] { () =>
        val invalidatedKeys = mutable.Set.empty[DamlStateKey]

        {
          case ValidatedSubmission(
                correlatedSubmission,
                inputState,
                logEntryAndOutputState,
                exporterWriteSet,
              ) =>
            detectConflictsAndRecover(
              correlatedSubmission,
              inputState,
              logEntryAndOutputState,
              invalidatedKeys,
              exporterWriteSet,
            )
        }
      }
      // Commit the results. This must be done serially to ensure a deterministic set of writes.
      .mapAsync[Outputs6](1) {
        case ValidatedSubmission(
              correlatedSubmission,
              inputState,
              logEntryAndOutputState,
              exporterWriteSet,
            ) =>
          commitResult(
            participantId,
            correlatedSubmission,
            inputState,
            logEntryAndOutputState,
            commitStrategy,
            exporterWriteSet,
          )
      }
      .runWith(Sink.ignore)
      .map(_ => exporterAggregator.finish())

  private def fetchSubmissionInputs(
      correlatedSubmission: CorrelatedSubmission,
      ledgerStateReader: DamlLedgerStateReader,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[FetchedInput] = {
    val inputKeys = correlatedSubmission.submission.getInputDamlStateList.asScala
    withSubmissionLoggingContext(correlatedSubmission) { _ =>
      Timed.timedAndTrackedFuture(
        metrics.fetchInputs,
        metrics.fetchInputsRunning,
        ledgerStateReader
          .read(inputKeys)
          .map { values =>
            (correlatedSubmission, inputKeys.zip(values).toMap)
          },
      )
    }
  }

  private def validateSubmission(
      participantId: Ref.ParticipantId,
      recordTime: Timestamp,
      correlatedSubmission: CorrelatedSubmission,
      inputState: DamlInputState,
      exporterWriteSet: SubmissionAggregator.WriteSetBuilder,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[ValidatedSubmission] =
    withSubmissionLoggingContext(correlatedSubmission) { _ =>
      Timed.timedAndTrackedFuture(
        metrics.validate,
        metrics.validateRunning,
        Future {
          val logEntryAndState = committer.processSubmission(
            correlatedSubmission.logEntryId,
            recordTime,
            LedgerReader.DefaultConfiguration,
            correlatedSubmission.submission,
            participantId,
            inputState,
          )
          ValidatedSubmission(correlatedSubmission, inputState, logEntryAndState, exporterWriteSet)
        },
      )
    }

  private def detectConflictsAndRecover(
      correlatedSubmission: CorrelatedSubmission,
      inputState: DamlInputState,
      logEntryAndState: LogEntryAndState,
      invalidatedKeys: mutable.Set[DamlStateKey],
      exporterWriteSet: SubmissionAggregator.WriteSetBuilder,
  ): scala.collection.immutable.Iterable[ValidatedSubmission] = {
    val (logEntry, outputState) = logEntryAndState
    withSubmissionLoggingContext(correlatedSubmission) { implicit loggingContext =>
      Timed.value(
        metrics.detectConflicts, {
          conflictDetection
            .detectConflictsAndRecover(
              invalidatedKeys,
              inputState,
              logEntry,
              outputState,
            )
            .map { case (newInvalidatedKeys, (newLogEntry, newState)) =>
              invalidatedKeys ++= newInvalidatedKeys
              ValidatedSubmission(
                correlatedSubmission,
                inputState,
                (newLogEntry, newState),
                exporterWriteSet,
              ) :: Nil
            }
            .getOrElse {
              logger.info(
                s"Submission ${correlatedSubmission.correlationId} dropped as it conflicted and recovery was not possible"
              )
              Nil
            }
        },
      )
    }
  }

  private def commitResult(
      participantId: Ref.ParticipantId,
      correlatedSubmission: CorrelatedSubmission,
      inputState: DamlInputState,
      logEntryAndState: LogEntryAndState,
      commitStrategy: CommitStrategy[CommitResult],
      exporterWriteSet: SubmissionAggregator.WriteSetBuilder,
  )(implicit executionContext: ExecutionContext, loggingContext: LoggingContext): Future[Unit] = {
    val (logEntry, outputState) = logEntryAndState
    withSubmissionLoggingContext(correlatedSubmission) { _ =>
      Timed
        .timedAndTrackedFuture(
          metrics.commit,
          metrics.commitRunning,
          commitStrategy.commit(
            participantId,
            correlatedSubmission.correlationId,
            correlatedSubmission.logEntryId,
            logEntry,
            inputState,
            outputState,
            Some(exporterWriteSet),
          ),
        )
        .map(_ => ())
    }
  }
}
