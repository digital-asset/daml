// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.time.Clock

import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.validator.ValidationFailed.{MissingInputState, ValidationError}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.logging.LoggingContext
import com.digitalasset.logging.LoggingContext.withEnrichedLoggingContext

import scala.concurrent.{ExecutionContext, Future}

class ValidatingCommitter[LogResult](
    participantId: ParticipantId,
    clock: Clock,
    validator: SubmissionValidator[LogResult],
    postCommit: LogResult => Unit,
) {
  def commit(
      correlationId: String,
      envelope: Array[Byte],
  )(implicit executionContext: ExecutionContext, logCtx: LoggingContext): Future[SubmissionResult] =
    withEnrichedLoggingContext("correlationId" -> correlationId) { implicit logCtx =>
      validator
        .validateAndCommit(
          envelope,
          correlationId,
          Timestamp.assertFromInstant(clock.instant()),
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
