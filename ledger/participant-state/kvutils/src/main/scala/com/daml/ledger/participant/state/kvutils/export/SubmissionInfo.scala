// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.CorrelationId
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.Raw

case class SubmissionInfo(
    participantId: ParticipantId,
    correlationId: CorrelationId,
    submissionEnvelope: Raw.Value,
    recordTimeInstant: Instant,
)
