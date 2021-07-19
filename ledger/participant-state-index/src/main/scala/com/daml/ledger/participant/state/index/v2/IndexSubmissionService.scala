// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import java.time.Instant

import com.daml.ledger.api.domain.CommandId
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc]]
  */
trait IndexSubmissionService {
  def deduplicateCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult]

  def stopDeduplicatingCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Unit]
}
