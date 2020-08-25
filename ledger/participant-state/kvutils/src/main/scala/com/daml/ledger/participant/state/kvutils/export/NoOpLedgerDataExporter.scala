// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.CorrelationId
import com.daml.ledger.participant.state.v1.ParticipantId
import com.google.protobuf.ByteString

object NoOpLedgerDataExporter extends LedgerDataExporter {
  override def addSubmission(
      participantId: ParticipantId,
      correlationId: CorrelationId,
      submissionEnvelope: ByteString,
      recordTimeInstant: Instant,
  ): SubmissionAggregator = NoOpSubmissionAggregator
}
