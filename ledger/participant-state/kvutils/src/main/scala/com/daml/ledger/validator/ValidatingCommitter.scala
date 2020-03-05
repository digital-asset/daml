// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.ValidationFailed.{MissingInputState, ValidationError}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.logging.LoggingContext.newLoggingContext

import scala.concurrent.{ExecutionContext, Future}

class ValidatingCommitter[LogResult](
    participantId: ParticipantId,
    now: () => Instant,
    validator: SubmissionValidator[LogResult],
    postCommit: LogResult => Unit,
) {
  def commit(
      correlationId: String,
      submission: DamlSubmission,
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] =
    newLoggingContext("correlationId" -> correlationId) { implicit logCtx =>
      validator
        .validateAndCommitWithLoggingContext(
          submission,
          correlationId,
          Timestamp.assertFromInstant(now()),
          participantId,
        )
        .map {
          case Right(value) =>
            postCommit(value)
            SubmissionResult.Acknowledged
          case Left(MissingInputState(keys)) =>
            SubmissionResult.InternalError(
              s"Missing input state: ${keys.map(_.map("%02x".format(_)).mkString).mkString(", ")}")
          case Left(ValidationError(reason)) =>
            SubmissionResult.InternalError(reason)
        }
    }
}
