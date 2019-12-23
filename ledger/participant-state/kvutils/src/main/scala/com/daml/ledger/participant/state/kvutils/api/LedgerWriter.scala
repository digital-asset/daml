// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}

import scala.concurrent.Future

trait LedgerWriter {
  def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult]

  def participantId: ParticipantId

  def checkHealth(): HealthStatus = Healthy
}
