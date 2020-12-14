// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.api.health.ReportsHealth
import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}

import scala.concurrent.Future

import com.daml.metrics.TelemetryContext

/**
  * Defines how we initiate a commit to the ledger.
  *
  * For example, the implementation may call the committer node through RPC and transmit the
  * submission, or in case of an in-memory implementation the validator may be directly called.
  */
trait LedgerWriter extends ReportsHealth {

  /**
    * @return participant ID of the participant on which this LedgerWriter instance runs
    */
  def participantId: ParticipantId

  /**
    * Sends a submission to be committed to the ledger.
    *
    * @param correlationId correlation ID to be used for logging purposes
    * @param envelope      opaque submission; may be compressed
    * @param metadata      metadata associated to this particular commit
    * @return future for sending the submission; for possible results see
    *         [[com.daml.ledger.participant.state.v1.SubmissionResult]]
    */
  def commit(
      correlationId: String,
      envelope: Bytes,
      metadata: CommitMetadata,
  )(implicit telemetryContext: TelemetryContext): Future[SubmissionResult]
}
