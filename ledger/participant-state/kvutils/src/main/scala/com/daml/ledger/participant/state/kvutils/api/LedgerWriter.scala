// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.digitalasset.ledger.api.health.ReportsHealth

import scala.concurrent.Future

trait LedgerWriter extends ReportsHealth {
  def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult]

  def participantId: ParticipantId
}
