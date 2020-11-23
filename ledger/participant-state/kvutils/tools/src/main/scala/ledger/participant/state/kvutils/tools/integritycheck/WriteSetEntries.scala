// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.validator.DefaultStateKeySerializationStrategy
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

object WriteSetEntries {
  def checkReadable(keyBytes: Key, valueBytes: Value): Either[String, Unit] =
    Envelope.open(valueBytes) match {
      case Left(errorMessage) =>
        Left(s"Invalid value envelope: $errorMessage")
      case Right(Envelope.LogEntryMessage(logEntry)) =>
        val _ = DamlLogEntryId.parseFrom(keyBytes)
        if (logEntry.getPayloadCase == DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET)
          Left("Log entry payload not set.")
        else
          Right(())
      case Right(Envelope.StateValueMessage(value)) =>
        val key = DefaultStateKeySerializationStrategy.deserializeStateKey(keyBytes)
        if (key.getKeyCase == DamlStateKey.KeyCase.KEY_NOT_SET)
          Left("State key not set.")
        else if (value.getValueCase == DamlStateValue.ValueCase.VALUE_NOT_SET)
          Left("State value not set.")
        else
          Right(())
      case Right(Envelope.SubmissionMessage(submission)) =>
        Left(s"Unexpected submission message: $submission")
      case Right(Envelope.SubmissionBatchMessage(batch)) =>
        Left(s"Unexpected submission batch message: $batch")
    }
}
