// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlLogEntry, DamlLogEntryId}
import com.daml.ledger.participant.state.kvutils.{Bytes, DamlKvutils, Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.{
  StateKeySerializationStrategy,
  StateSerializationStrategy,
  inParallel
}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

final class LogAppenderPreExecutingCommitStrategy(
    keySerializationStrategy: StateKeySerializationStrategy,
) extends PreExecutingCommitStrategy[RawKeyValuePairsWithLogEntry] {
  private val stateSerializationStrategy = new StateSerializationStrategy(keySerializationStrategy)

  override def generateWriteSets(
      participantId: ParticipantId,
      logEntryId: DamlLogEntryId,
      inputState: Map[DamlKvutils.DamlStateKey, Option[DamlKvutils.DamlStateValue]],
      preExecutionResult: KeyValueCommitting.PreExecutionResult,
  )(implicit executionContext: ExecutionContext)
    : Future[PreExecutionCommitResult[RawKeyValuePairsWithLogEntry]] = {
    for {
      (
        serializedSuccessKeyValuePairs,
        (serializedSuccessLogEntryPair, serializedOutOfTimeBoundsLogEntryPair),
      ) <- inParallel(
        Future(stateSerializationStrategy.serializeStateUpdates(preExecutionResult.stateUpdates)),
        Future(logEntryId.toByteString).flatMap(
          serializedId =>
            inParallel(
              logEntryToKeyValuePairs(serializedId, preExecutionResult.successfulLogEntry),
              logEntryToKeyValuePairs(serializedId, preExecutionResult.outOfTimeBoundsLogEntry),
          )),
      )
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
          serializedOutOfTimeBoundsLogEntryPair._2,
        ),
        // We assume updates for a successful transaction must be visible to every participant for
        // public ledgers.
        involvedParticipants = Set.empty,
      )
  }

  private def logEntryToKeyValuePairs(
      logEntryId: ByteString,
      logEntry: DamlLogEntry,
  )(implicit executionContext: ExecutionContext): Future[(Bytes, Bytes)] =
    Future(logEntryId -> Envelope.enclose(logEntry))
}
