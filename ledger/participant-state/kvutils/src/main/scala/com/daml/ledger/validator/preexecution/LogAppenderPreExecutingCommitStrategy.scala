// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.SubmissionValidator.RawKeyValuePairs

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
      envelopedSuccessLogEntry <- Future {
        Envelope.enclose(preExecutionResult.successfulLogEntry)
      }
      serializedLogEntryId = entryId.toByteString
      serializedSuccessLogEntryPair = Seq((serializedLogEntryId, envelopedSuccessLogEntry))
      envelopedOutOfTimeBoundsLogEntry <- Future {
        Envelope.enclose(preExecutionResult.outOfTimeBoundsLogEntry)
      }
      serializedOutOfTimeBoundsLogEntryPair = Seq(
        (serializedLogEntryId, envelopedOutOfTimeBoundsLogEntry))
    } yield
      PreExecutionCommitResult(
        successWriteSet = serializedSuccessKeyValuePairs ++ serializedSuccessLogEntryPair,
        outOfTimeBoundsWriteSet = serializedOutOfTimeBoundsLogEntryPair,
        // We assume updates for a successful transaction must be visible to every participant for
        // public ledgers.
        involvedParticipants = Set.empty
      )
  }
}
