// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.kvutils.export.SubmissionAggregator
import com.daml.ledger.participant.state.v1.ParticipantId

import scala.collection.breakOut
import scala.concurrent.{ExecutionContext, Future}

class LogAppendingCommitStrategy[Index](
    ledgerStateOperations: LedgerStateOperations[Index],
    keySerializationStrategy: StateKeySerializationStrategy,
)(implicit executionContext: ExecutionContext)
    extends CommitStrategy[Index] {
  override def commit(
      participantId: ParticipantId,
      correlationId: String,
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      outputState: Map[DamlStateKey, DamlStateValue],
      exporterWriteSet: Option[SubmissionAggregator.WriteSetBuilder] = None,
  ): Future[Index] =
    for {
      serializedKeyValuePairs <- Future.successful(outputState.map {
        case (key, value) =>
          (keySerializationStrategy.serializeStateKey(key), Envelope.enclose(value))
      }(breakOut))
      _ = exporterWriteSet.foreach {
        _ ++= serializedKeyValuePairs
      }
      _ <- if (serializedKeyValuePairs.nonEmpty) {
        ledgerStateOperations.writeState(serializedKeyValuePairs)
      } else {
        Future.unit
      }
      envelopedLogEntry <- Future.successful(Envelope.enclose(entry))
      _ = exporterWriteSet.foreach {
        _ += entryId.toByteString -> envelopedLogEntry
      }
      index <- ledgerStateOperations
        .appendToLog(
          entryId.toByteString,
          envelopedLogEntry
        )
    } yield index
}
