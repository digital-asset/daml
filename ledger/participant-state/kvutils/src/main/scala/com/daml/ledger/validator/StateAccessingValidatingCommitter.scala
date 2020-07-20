// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}

import scala.concurrent.{ExecutionContext, Future}

trait StateAccessingValidatingCommitter[LogResult] {
  def commit(
      correlationId: String,
      envelope: Bytes,
      submittingParticipantId: ParticipantId,
      ledgerStateAccess: LedgerStateAccess[LogResult],
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult]
}
