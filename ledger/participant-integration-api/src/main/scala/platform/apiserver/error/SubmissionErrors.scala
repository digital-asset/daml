// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.error

import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.platform.apiserver.error.ErrorGroups.ParticipantErrorGroup.TransactionErrorGroup.SubmissionErrorGroup
import org.slf4j.event.Level

object SubmissionErrors extends SubmissionErrorGroup {

  // TODO(i5990) split the text into sub-categories with codes
  @Explanation("""This error has not yet been properly categorised into sub-error codes.""")
  object MalformedRequest
      extends ErrorCode(
        id = "MALFORMED_REQUEST",
        ErrorCategory.InvalidIndependentOfSystemState,
      ) {

    // TODO error codes: properly set `definiteAnswer` where appropriate when sub-categories are created
    case class Error(message: String) extends TransactionErrorImpl(cause = "Malformed request")
//        with TransactionSubmissionError // TODO error codes: Validate if mixin of this trait is needed
  }

  @Explanation(
    """This error occurs when the participant rejects a command due to excessive load.
      |Load can be caused as follows:
      |1. when commands are submitted to the participant through its ledger api, 
      |2. when the participant receives requests from other participants through a connected domain."""
  )
  @Resolution(
    """Wait a bit and retry, preferably with some backoff factor.
      |If possible, ask other participants to send fewer requests; the domain operator can enforce this by imposing a rate limit."""
  )
  object ParticipantBackpressure
      extends ErrorCode(
        id = "PARTICIPANT_BACKPRESSURE",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    override protected def logLevel: Level = Level.WARN

    case class Rejection(reason: String)
        extends TransactionErrorImpl(cause = "The participant is overloaded")
  }

  @Explanation(
    """This error occurs when a command is submitted while the system is performing a shutdown."""
  )
  @Resolution(
    "Assuming that the participant will restart or failover eventually, retry in a couple of seconds."
  )
  object SubmissionDuringShutdown
      extends ErrorCode(
        id = "SUBMISSION_DURING_SHUTDOWN",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    case class Rejection() extends TransactionErrorImpl(cause = "Command rejected due to shutdown.")
//        with TransactionSubmissionError // TODO error codes: Validate if mixin of this trait is needed
  }
}
