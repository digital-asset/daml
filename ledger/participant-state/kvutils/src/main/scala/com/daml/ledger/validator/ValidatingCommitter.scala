// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.validator.ValidationFailed.{MissingInputState, ValidationError}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext.newLoggingContextWith
import com.google.rpc.code.Code
import com.google.rpc.status.Status

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/** Orchestrates committing to a ledger after validating the submission.
  * Example usage, assuming a [[com.daml.ledger.participant.state.kvutils.api.LedgerWriter]] sends the submission over
  * the wire:
  * {{{
  *   ...
  *   private val ledgerStateAccess = ...
  *   private val validator = SubmissionValidator.create(ledgerStateAccess)
  *   private val validatingCommitter = new ValidatingCommitter(
  *       myParticipantId,
  *       () => Instant.now(),
  *       validator,
  *       signalDispatcher)
  *   ...
  *
  *   def commitRequestHandler(request: CommitRequest): Future[CommitResponse] =
  *     validatingCommitter.commit(request.correlationId, request.envelope)
  *       .map(...)
  * }}}
  *
  * @param now function implementing resolution of current time when processing submission
  * @param validator validator instance to use
  * @param postCommit  function called after a successful commit, e.g., this can be used to signal readers that a new log
  *                    entry is available
  * @tparam LogResult  type of the offset used for a log entry
  */
class ValidatingCommitter[LogResult](
    now: () => Instant,
    validator: SubmissionValidator[LogResult],
    postCommit: LogResult => Unit,
) {
  def commit(
      correlationId: String,
      envelope: Raw.Envelope,
      submittingParticipantId: Ref.ParticipantId,
  )(implicit executionContext: ExecutionContext): Future[SubmissionResult] =
    newLoggingContextWith("correlationId" -> correlationId) { implicit loggingContext =>
      validator
        .validateAndCommitWithContext(
          envelope,
          correlationId,
          Timestamp.assertFromInstant(now()),
          submittingParticipantId,
        )
        .map {
          case Right(value) =>
            postCommit(value)
            SubmissionResult.Acknowledged
          case Left(MissingInputState(keys)) =>
            // TODO error codes: Adapt V2 ?
            SubmissionResult.SynchronousError(
              Status(
                Code.INTERNAL.value,
                s"Missing input state: ${keys.map(_.bytes.asScala.map("%02x".format(_)).mkString).mkString(", ")}",
              )
            )
          case Left(ValidationError(reason)) =>
            // TODO error codes: Adapt V2 ?
            SubmissionResult.SynchronousError(
              Status(
                Code.INTERNAL.value,
                reason,
              )
            )
        }
    }
}
