// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import java.time.Instant
import scala.concurrent.Future

/**
  * Serves as a backend to implement
  * [[com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc]]
  **/
trait IndexSubmissionService {
  def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      ttl: Instant): Future[CommandDeduplicationResult]

  def updateCommandResult(
      deduplicationKey: String,
      submittedAt: Instant,
      result: CommandSubmissionResult): Future[Unit]
}
