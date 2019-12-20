package com.daml.ledger.participant.state.kvutils.api

import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}

import scala.concurrent.Future

trait LedgerWriter {
  //def allocateLogEntryId: Array[Byte]

  //def commit(correlationId: String, logEntryId: Array[Byte], envelope: Array[Byte]): Future[SubmissionResult]
  // How to combine state+log updates? Without that there's no point in a single commit method.
  // This is only for boundary participant -> committer, not the committer implementation itslef!
  def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult]

  def participantId: ParticipantId

  def checkHealth(): HealthStatus = Healthy
}
