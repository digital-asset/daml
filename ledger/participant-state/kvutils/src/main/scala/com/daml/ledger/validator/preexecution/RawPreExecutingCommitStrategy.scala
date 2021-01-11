// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueCommitting, Raw}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.preexecution.RawPreExecutingCommitStrategy.{InputState, ReadSet}
import com.daml.ledger.validator.{
  StateKeySerializationStrategy,
  StateSerializationStrategy,
  inParallel,
}

import scala.concurrent.{ExecutionContext, Future}

final class RawPreExecutingCommitStrategy(
    keySerializationStrategy: StateKeySerializationStrategy
) extends PreExecutingCommitStrategy[
      DamlStateKey,
      Option[DamlStateValue],
      ReadSet,
      RawKeyValuePairsWithLogEntry,
    ] {
  private val stateSerializationStrategy = new StateSerializationStrategy(keySerializationStrategy)

  override def generateReadSet(
      fetchedInputs: InputState,
      accessedKeys: Set[DamlStateKey],
  ): Map[DamlStateKey, Option[DamlStateValue]] =
    accessedKeys.view.map { key =>
      val value =
        fetchedInputs.getOrElse(key, throw new KeyNotPresentInInputException(key))
      key -> value
    }.toMap

  override def generateWriteSets(
      participantId: ParticipantId,
      logEntryId: DamlLogEntryId,
      inputState: InputState,
      preExecutionResult: KeyValueCommitting.PreExecutionResult,
  )(implicit
      executionContext: ExecutionContext
  ): Future[PreExecutionCommitResult[RawKeyValuePairsWithLogEntry]] = {
    for {
      (
        serializedSuccessKeyValuePairs,
        (serializedSuccessLogEntryPair, serializedOutOfTimeBoundsLogEntryPair),
      ) <- inParallel(
        Future(stateSerializationStrategy.serializeStateUpdates(preExecutionResult.stateUpdates)),
        Future(Raw.Key(logEntryId.toByteString)).flatMap(serializedId =>
          inParallel(
            logEntryToKeyValuePairs(serializedId, preExecutionResult.successfulLogEntry),
            logEntryToKeyValuePairs(serializedId, preExecutionResult.outOfTimeBoundsLogEntry),
          )
        ),
      )
    } yield PreExecutionCommitResult(
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
      logEntryId: Raw.Key,
      logEntry: DamlLogEntry,
  )(implicit executionContext: ExecutionContext): Future[Raw.KeyValuePair] =
    Future(logEntryId -> Envelope.enclose(logEntry))
}

object RawPreExecutingCommitStrategy {
  type InputState = Map[DamlStateKey, Option[DamlStateValue]]
  type ReadSet = Map[DamlStateKey, Option[DamlStateValue]]
}
