// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.{ConflictDetection, Envelope}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.SubmissionValidator._
import com.digitalasset.daml.lf.data.Time.Timestamp

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
class BatchValidator[LogResult](
    val params: BatchValidationParameters,
    val allocateLogEntryId: () => DamlLogEntryId,
    val ledgerStateAccess: LedgerStateAccess[LogResult]) {

  def validate[T](recordTime: Timestamp, participantId: ParticipantId, envelope: Array[Byte])(
      implicit materializer: Materializer) = {
    implicit val executionContext: ExecutionContext = materializer.executionContext
    Envelope.open(envelope) match {
      case Right(Envelope.BatchMessage(batch)) =>
        // TODO(JM): Here we're doing everything in one transaction. In principle if the assumption holds
        // that there's a single validator that sequentially processes the submissions, then we can do the
        // reads and writes in separate transactions as we can assume there's no other writers. This assumption
        // would break with DAML-on-SQL with multiple writers.
        ledgerStateAccess.inTransaction { stateOperations =>
          Source
            .fromIterator(() => batch.getSubmissionsList.asScala.toIterator)

            // Uncompress the submissions in parallel.
            .mapAsyncUnordered(params.cpuParallelism)(cs =>
              Future {
                //println("UNCOMPRESS")
                cs.getCorrelationId -> Envelope
                  .openSubmission(cs.getSubmission)
                  .right
                  .get /* FIXME what to do? */
            })

            // Fetch the submission inputs in parallel.
            // NOTE(JM): We assume the underlying ledger state access caches reads within transaction
            // and hence make no effort here to deduplicate reads.
            // FIXME(JM): Since we're wrapped within "inTransaction" doing these in parallel may or may not
            // help. See comment on top about using separate ledger state transactions.
            .mapAsyncUnordered(params.ioParallelism) {
              case (corId, subm) =>
                val inputKeys = subm.getInputDamlStateList.asScala
                stateOperations
                  .readState(inputKeys.map(keyToBytes))
                  .map { values =>
                    //println("READ STATE")
                    (
                      corId,
                      subm,
                      values
                        .zip(inputKeys)
                        .map {
                          case (valueBytes, key) => key -> valueBytes.map(bytesToStateValue)
                        }
                        .toMap)
                  }
            }

            // Validate the submissions in parallel.
            .mapAsyncUnordered(params.cpuParallelism) {
              case (corId, subm, inputState) =>
                Future {
                  val damlLogEntryId = allocateLogEntryId()
                  //println("VALIDATE")
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
              case (logEntryId, logEntryAndState) =>
                //println("SERIALIZE AND WRITE")
                val (rawLogEntry, rawStateUpdates) =
                  serializeProcessedSubmission(logEntryAndState)
                for {
                  logResult <- stateOperations.appendToLog(logEntryId.toByteArray, rawLogEntry)
                  _ <- if (rawStateUpdates.nonEmpty)
                    stateOperations.writeState(rawStateUpdates)
                  else Future.unit
                } yield logResult
            }
            .runWith(Sink.seq)
        }

      case _ =>
        sys.error("Unsupported message type")
    }
  }

}
