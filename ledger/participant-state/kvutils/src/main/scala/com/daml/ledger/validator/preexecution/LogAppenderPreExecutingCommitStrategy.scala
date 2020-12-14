// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.{Bytes, Envelope, Fingerprint, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.preexecution.PreExecutionCommitResult.ReadSet
import com.daml.ledger.validator.{
  StateKeySerializationStrategy,
  StateSerializationStrategy,
  inParallel
}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

final class LogAppenderPreExecutingCommitStrategy(
    keySerializationStrategy: StateKeySerializationStrategy,
) extends PreExecutingCommitStrategy[
      DamlStateKey,
      (Option[DamlStateValue], Fingerprint),
      ReadSet,
      RawKeyValuePairsWithLogEntry,
    ] {
  private val stateSerializationStrategy = new StateSerializationStrategy(keySerializationStrategy)

  override def generateReadSet(
      fetchedInputs: Map[DamlStateKey, (Option[DamlStateValue], Fingerprint)],
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

  override def generateWriteSets(
      participantId: ParticipantId,
      logEntryId: DamlLogEntryId,
      inputState: Map[DamlStateKey, (Option[DamlStateValue], Fingerprint)],
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
