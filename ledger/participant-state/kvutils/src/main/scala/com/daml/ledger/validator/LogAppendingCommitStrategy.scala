// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlLogEntry, DamlLogEntryId}
import com.daml.ledger.participant.state.kvutils.DamlState.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.{ExecutionContext, Future}

class LogAppendingCommitStrategy[Index](
    ledgerStateOperations: LedgerStateOperations[Index],
    keySerializationStrategy: StateKeySerializationStrategy,
)(implicit executionContext: ExecutionContext)
    extends CommitStrategy[Index] {
  private val stateSerializationStrategy = new StateSerializationStrategy(keySerializationStrategy)

  override def commit(
      participantId: Ref.ParticipantId,
      correlationId: String,
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      outputState: Map[DamlStateKey, DamlStateValue],
      writeSetBuilder: Option[SubmissionAggregator.WriteSetBuilder] = None,
  )(implicit loggingContext: LoggingContext): Future[Index] = {
    val rawLogEntryId = Raw.LogEntryId(entryId)
    for {
      (serializedKeyValuePairs, envelopedLogEntry) <- inParallel(
        Future(stateSerializationStrategy.serializeStateUpdates(outputState)),
        Future(Envelope.enclose(entry)),
      )
      (_, _, index) <- inParallel(
        if (serializedKeyValuePairs.nonEmpty) {
          ledgerStateOperations.writeState(serializedKeyValuePairs)
        } else {
          Future.unit
        },
        Future {
          writeSetBuilder.foreach { builder =>
            builder ++= serializedKeyValuePairs
            builder += rawLogEntryId -> envelopedLogEntry
          }
        },
        ledgerStateOperations.appendToLog(rawLogEntryId, envelopedLogEntry),
      )
    } yield index
  }
}
