// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      deduplicateUntil: Instant): Future[CommandDeduplicationResult]
}
