// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlLogEntry, DamlLogEntryId}
import com.daml.ledger.participant.state.kvutils.{
  Bytes,
  DamlKvutils,
  Envelope,
  KeyValueCommitting,
  `Bytes Ordering`
}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.google.protobuf.ByteString

import scala.collection.{SortedMap, breakOut}
import scala.concurrent.{ExecutionContext, Future}

class LogAppenderPreExecutingCommitStrategy(keySerializationStrategy: StateKeySerializationStrategy)
    extends PreExecutingCommitStrategy[RawKeyValuePairsWithLogEntry] {

  override def generateWriteSets(
      participantId: ParticipantId,
      entryId: DamlLogEntryId,
      inputState: Map[DamlKvutils.DamlStateKey, Option[DamlKvutils.DamlStateValue]],
      preExecutionResult: KeyValueCommitting.PreExecutionResult,
  )(implicit executionContext: ExecutionContext)
    : Future[PreExecutionCommitResult[RawKeyValuePairsWithLogEntry]] = {
    val serializedSuccessKeyValuePairs: SortedMap[Key, Value] =
      preExecutionResult.stateUpdates
        .map {
          case (key, value) =>
            (keySerializationStrategy.serializeStateKey(key), Envelope.enclose(value))
        }(breakOut)
    val serializedLogEntryId = entryId.toByteString
    for {
      serializedSuccessLogEntryPair <- logEntryToKeyValuePairs(
        serializedLogEntryId,
        preExecutionResult.successfulLogEntry)
      serializedOutOfTimeBoundsLogEntryPair <- logEntryToKeyValuePairs(
        serializedLogEntryId,
        preExecutionResult.outOfTimeBoundsLogEntry)
    } yield
      PreExecutionCommitResult(
        successWriteSet = RawKeyValuePairsWithLogEntry(
          serializedSuccessKeyValuePairs,
          serializedSuccessLogEntryPair._1,
          serializedSuccessLogEntryPair._2,
        ),
        outOfTimeBoundsWriteSet = RawKeyValuePairsWithLogEntry(
          Seq.empty,
          serializedOutOfTimeBoundsLogEntryPair._1,
          serializedOutOfTimeBoundsLogEntryPair._2),
        // We assume updates for a successful transaction must be visible to every participant for
        // public ledgers.
        involvedParticipants = Set.empty
      )
  }

  private def logEntryToKeyValuePairs(
      logEntryId: ByteString,
      logEntry: DamlLogEntry,
  )(implicit executionContext: ExecutionContext): Future[(Bytes, Bytes)] = Future {
    logEntryId -> Envelope.enclose(logEntry)
  }
}
