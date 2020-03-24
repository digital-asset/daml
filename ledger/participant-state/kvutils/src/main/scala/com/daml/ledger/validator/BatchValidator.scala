// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.time.Instant

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.api.LedgerReader
import com.daml.ledger.participant.state.kvutils.{ConflictDetection, Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.SubmissionValidator.LogEntryAndState
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

/** Parameters for batch validation.
  *
  * @param cpuParallelism Amount of parallelism to use for CPU bound steps.
  * @param ioParallelism Amount of parallelism to use for ledger state operations.
  */
case class BatchValidationParameters(
    cpuParallelism: Int,
    ioParallelism: Int
)

object BatchValidationParameters {
  // FIXME
  def default: BatchValidationParameters = BatchValidationParameters(8, 4)
}

// This is example code for validating a batch of submissions in parallel, including conflict detection.
class BatchValidator(
    val params: BatchValidationParameters,
    val allocateLogEntryId: () => DamlLogEntryId,
    val ledgerState: LedgerState) {

  private val engine: Engine = Engine()

  // TODO(JM): How to report errors? Future[Unit] that fails or Future[Either[ValidationError, Unit]] that never
  // fails? We're expecting this to fail only when a batch is corrupted or malicious (and out of memory etc.).
  def validateAndCommit(
      recordTimeInstant: Instant,
      participantId: ParticipantId,
      envelope: ByteString)(implicit materializer: Materializer) = {

    val recordTime = Time.Timestamp.assertFromInstant(recordTimeInstant)

    implicit val executionContext: ExecutionContext = materializer.executionContext
    Envelope.open(envelope) match {
      case Right(Envelope.BatchMessage(batch)) =>
        // TODO(JM): Here we're doing everything in one transaction. In principle if the assumption holds
        // that there's a single validator that sequentially processes the submissions, then we can do the
        // reads and writes in separate transactions as we can assume there's no other writers. This assumption
        // would break with DAML-on-SQL with multiple writers.
        ledgerState.inTransaction { stateOps =>
          Source
            .fromIterator(() => batch.getSubmissionsList.asScala.toIterator)

            // Uncompress the submissions in parallel.
            .mapAsyncUnordered(params.cpuParallelism)(cs =>
              Future {
                println("UNCOMPRESS")
                cs.getCorrelationId -> Envelope
                  .openSubmission(cs.getSubmission)
                  .fold(err => throw ValidationFailed.ValidationError(err), identity)
            })

            // Fetch the submission inputs in parallel.
            // NOTE(JM): We assume the underlying ledger state access caches reads within transaction
            // and hence make no effort here to deduplicate reads.
            // FIXME(JM): Since we're wrapped within "inTransaction" doing these in parallel may or may not
            // help. See comment on top about using separate ledger state transactions.
            .mapAsyncUnordered(params.ioParallelism) {
              case (corId, subm) =>
                val inputKeys = subm.getInputDamlStateList.asScala
                stateOps
                  .readState(inputKeys)
                  .map { values =>
                    println("READ STATE")
                    (
                      corId,
                      subm,
                      inputKeys.zip(values).toMap
                    )
                  }
            }

            // Validate the submissions in parallel.
            .mapAsyncUnordered(params.cpuParallelism) {
              case (corId, subm, inputState) =>
                Future {
                  val damlLogEntryId = allocateLogEntryId()
                  println("VALIDATE")
                  (
                    damlLogEntryId,
                    processSubmission(damlLogEntryId, recordTime, subm, participantId, inputState),
                    inputState.keySet)
                }
            }

            // Detect conflicts and choose the appropriate result.
            .statefulMapConcat { () =>
              var modifiedKeys = Set.empty[DamlStateKey]

              {
                case (logEntryId, logEntryAndState, inputKeys) =>
                  ConflictDetection.conflictDetectAndRecover(
                    modifiedKeys,
                    inputKeys,
                    logEntryAndState._1,
                    logEntryAndState._2
                  ) match {
                    case Some((newLogEntry, newState)) =>
                      modifiedKeys ++= newState.keySet
                      (logEntryId, (newLogEntry, newState)) :: Nil

                    case None =>
                      Nil
                  }
              }
            }

            // Serialize and commit in parallel.
            .mapAsyncUnordered(params.ioParallelism) {
              case (logEntryId, (logEntry, outputState)) =>
                println("SERIALIZE AND WRITE")
                stateOps
                  .appendToLog(logEntryId, logEntry)
                  .flatMap(_ => stateOps.writeState(outputState.toSeq))
            }
            .runWith(Sink.ignore)
            .map(_ => ())
        }

      case _ =>
        sys.error("Unsupported message type")
    }
  }

  private[validator] def processSubmission(
      damlLogEntryId: DamlLogEntryId,
      recordTime: Timestamp,
      damlSubmission: DamlSubmission,
      participantId: ParticipantId,
      inputState: Map[DamlStateKey, Option[DamlStateValue]]): LogEntryAndState =
    KeyValueCommitting.processSubmission(
      engine,
      damlLogEntryId,
      recordTime,
      LedgerReader.DefaultConfiguration,
      damlSubmission,
      participantId,
      inputState
    )

}
