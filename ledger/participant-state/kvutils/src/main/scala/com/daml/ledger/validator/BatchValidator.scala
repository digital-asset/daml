// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.SubmissionValidator._
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.logging.ContextualizedLogger
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

// This is example code for validating a batch of submissions in parallel, including conflict detection.
class BatchValidator[LogResult](
    val allocateLogEntryId: () => DamlLogEntryId,
    val ledgerStateAccess: LedgerStateAccess[LogResult]) {

  // Queue for validation work.
  val validationQueue = new ArrayBlockingQueue[Runnable](1024)
  val nThreads = 2 * Runtime.getRuntime.availableProcessors()

  // Executor that performs the validations.
  // TODO(JM): We probably don't need anything this complicated. A fixed thread pool should be fine.
  // Or if we want to keep things simpler we can just use an existing thread pool / execution context,
  // though it would be useful for profiling purposes to have a dedicated thread pool.
  val executor = new ThreadPoolExecutor(
    nThreads,
    nThreads, // pool size
    1, // thread keep-alive time
    TimeUnit.SECONDS,
    validationQueue,
    new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("batch-validator-%d")
      .build(),
    new ThreadPoolExecutor.CallerRunsPolicy
  )

  // The execution context in which we execute the futures.
  implicit val batchExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutor(executor, e => throw e)

  private val logger = ContextualizedLogger.get(getClass)

  private val engine = Engine()

  // TODO(JM): This thing is a bit silly. Figure out a cleaner way.
  case class LogResultsCollector(
      private val results: List[LogResult],
      private val modifiedKeys: Set[DamlStateKey]) {
    def commit(
        logEntryId: DamlLogEntryId,
        logEntryAndState: LogEntryAndState,
        stateOperations: LedgerStateOperations[LogResult]): Future[LogResultsCollector] = {
      val outputKeys = logEntryAndState._2.keySet
      if (hasConflict(outputKeys)) {
        // FIXME(JM): We need a rejection log entry
        //Future.successful(this)
        sys.error("CONFLICT IN BATCH!")
      } else {
        val (rawLogEntry, rawStateUpdates) =
          serializeProcessedSubmission(logEntryAndState)
        for {
          logResult <- stateOperations.appendToLog(logEntryId.toByteArray, rawLogEntry)
          _ <- if (rawStateUpdates.nonEmpty)
            stateOperations.writeState(rawStateUpdates)
          else Future.unit
        } yield addResult(logResult)
      }
    }

    def getResults: Iterable[LogResult] = results.reverse

    private def addResult(result: LogResult) = this.copy(results = result :: results)

    private def hasConflict(keys: Set[DamlStateKey]) =
      keys.find(modifiedKeys.contains).isDefined
  }

  object LogResultsCollector {
    def empty: LogResultsCollector = LogResultsCollector(List.empty, Set.empty)
  }

  def validate[T](recordTime: Timestamp, participantId: ParticipantId, envelope: Array[Byte]) =
    Envelope.open(envelope) match {
      case Right(Envelope.BatchMessage(batch)) =>
        // TODO(JM): Consider whether akka-streams allows writing this more succintly. I gave it a try and gave up.
        for {
          // Unpack the submissions in parallel.
          correlatedSubmissions <- Future.sequence(
            batch.getSubmissionsList.asScala.map(cs =>
              Future {
                cs.getCorrelationId -> Envelope.openSubmission(cs.getSubmission).right.get
            })
          )

          // Collect all required inputs.
          allDeclaredInputs = correlatedSubmissions.flatMap(_._2.getInputDamlStateList.asScala)

          // Process the submissions in a single ledger state transaction and commit.
          results <- ledgerStateAccess.inTransaction { stateOperations =>
            for {
              // Fetch all input state for the batch in one go.
              readStateValues <- stateOperations.readState(allDeclaredInputs.map(keyToBytes))
              readStateInputs = readStateValues.zip(allDeclaredInputs).map {
                case (valueBytes, key) => (key, valueBytes.map(bytesToStateValue))
              }

              // Validate the submissions in parallel.
              // TODO(JM): Verify that this actually runs in batchExecutionContext and that these run in parallel.
              logEntriesAndStates <- Future.sequence(correlatedSubmissions.map {
                case (correlationId, submission) =>
                  val damlLogEntryId = allocateLogEntryId()
                  val readInputs: Map[DamlStateKey, Option[DamlStateValue]] = readStateInputs.toMap
                  Future.fromTry(
                    Try(
                      damlLogEntryId ->
                        processSubmission(
                          damlLogEntryId,
                          recordTime,
                          submission,
                          participantId,
                          readInputs)))
              })

              // Conflict detect and commit in sequence.
              collector <- logEntriesAndStates.foldLeft(Future(LogResultsCollector.empty)) {
                case (collectorF, (logEntryId, logEntryAndState)) =>
                  collectorF.flatMap(_.commit(logEntryId, logEntryAndState, stateOperations))
              }
            } yield collector.getResults
          }

        } yield results

      case _ =>
        sys.error("Unsupported message type")
    }

}
