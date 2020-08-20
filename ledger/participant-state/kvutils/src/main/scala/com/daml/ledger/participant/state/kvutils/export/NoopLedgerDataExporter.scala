// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.time.Instant

import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString

object NoopLedgerDataExporter extends LedgerDataExporter {
  override def addSubmission(
      submissionEnvelope: ByteString,
      correlationId: String,
      recordTimeInstant: Instant,
      participantId: ParticipantId,
  ): Unit = ()

  override def addParentChild(parentCorrelationId: String, childCorrelationId: String): Unit = ()

  override def addToWriteSet(correlationId: String, data: Iterable[(Key, Value)]): Unit = ()

  override def finishedProcessing(correlationId: String): Unit = ()
}
