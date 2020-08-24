// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger

import com.daml.ledger.participant.state.kvutils.{Bytes, CorrelationId}
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}

import scala.concurrent.Future

package object validator {
  type SubmissionEnvelope = Bytes
  type SubmittingParticipantId = ParticipantId

  /**
    * Orchestrates committing to a ledger after validating submissions.
    */
  type ValidateAndCommit = (
      CorrelationId,
      SubmissionEnvelope,
      SubmittingParticipantId,
  ) => Future[SubmissionResult]
}
