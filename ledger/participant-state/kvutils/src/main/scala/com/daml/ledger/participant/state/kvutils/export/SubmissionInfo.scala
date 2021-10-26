// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import com.daml.ledger.participant.state.kvutils.{CorrelationId, Raw}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp

case class SubmissionInfo(
    participantId: Ref.ParticipantId,
    correlationId: CorrelationId,
    submissionEnvelope: Raw.Envelope,
    recordTime: Timestamp,
)
