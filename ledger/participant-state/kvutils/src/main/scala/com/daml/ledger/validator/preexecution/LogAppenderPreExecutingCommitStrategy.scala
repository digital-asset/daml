// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlLogEntry, DamlLogEntryId}
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.SubmissionValidator.RawKeyValuePairs
import com.google.protobuf.ByteString

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}

class LogAppenderPreExecutingCommitStrategy(keySerializationStrategy: StateKeySerializationStrategy)(
    implicit executionContext: ExecutionContext)
    extends PreExecutingCommitStrategy[RawKeyValuePairs] {
  override def generateWriteSets(
      participantId: ParticipantId,
      entryId: DamlLogEntryId,
      inputState: Map[DamlKvutils.DamlStateKey, Option[DamlKvutils.DamlStateValue]],
      preExecutionResult: KeyValueCommitting.PreExecutionResult)
    : Future[PreExecutionCommitResult[RawKeyValuePairs]] = {
    for {
      serializedSuccessKeyValuePairs <- Future {
        preExecutionResult.stateUpdates.map {
          case (key, value) =>
            (keySerializationStrategy.serializeStateKey(key), Envelope.enclose(value))
        }(breakOut)
      }
      serializedLogEntryId = entryId.toByteString
      serializedSuccessLogEntryPair <- logEntryToKeyValuePairs(
        serializedLogEntryId,
        preExecutionResult.successfulLogEntry)
      serializedOutOfTimeBoundsLogEntryPair <- logEntryToKeyValuePairs(
        serializedLogEntryId,
        preExecutionResult.outOfTimeBoundsLogEntry)
    } yield
      PreExecutionCommitResult(
        successWriteSet = serializedSuccessKeyValuePairs ++ serializedSuccessLogEntryPair,
        outOfTimeBoundsWriteSet = serializedOutOfTimeBoundsLogEntryPair,
        // We assume updates for a successful transaction must be visible to every participant for
        // public ledgers.
        involvedParticipants = Set.empty
      )
  }

  private def logEntryToKeyValuePairs(
      logEntryId: ByteString,
      logEntry: DamlLogEntry): Future[RawKeyValuePairs] = Future {
    val envelopedLogEntry = Envelope.enclose(logEntry)
    Seq((logEntryId, envelopedLogEntry))
  }
}
