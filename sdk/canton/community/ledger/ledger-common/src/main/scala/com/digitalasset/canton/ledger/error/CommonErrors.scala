// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error

import com.daml.error.*
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.CommonErrorGroup

import scala.concurrent.duration.Duration

@Explanation(
  "Common errors raised in Daml services and components."
)
object CommonErrors extends CommonErrorGroup {

  @Explanation(
    """This error category is used to signal that an unimplemented code-path has been triggered by a client or participant operator request."""
  )
  @Resolution(
    """This error is caused by a participant node misconfiguration or by an implementation bug.
      |Resolution requires participant operator intervention."""
  )
  object UnsupportedOperation
      extends ErrorCode(
        id = "UNSUPPORTED_OPERATION",
        ErrorCategory.InternalUnsupportedOperation,
      ) {

    final case class Reject(message: String)(implicit errorLogger: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          cause = s"The request exercised an unsupported operation: $message"
        )
  }

  @Explanation(
    "This rejection is given when a request processing status is not known and a time-out is reached."
  )
  @Resolution(
    "Retry for transient problems. If non-transient contact the operator as the time-out limit might be too short."
  )
  object RequestTimeOut
      extends ErrorCode(
        id = "REQUEST_TIME_OUT",
        ErrorCategory.DeadlineExceededRequestStateUnknown,
      ) {
    final case class Reject(message: String, override val definiteAnswer: Boolean)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = message,
          definiteAnswer = definiteAnswer,
        )
  }

  @Explanation(
    "The request has not been submitted for processing as its predefined deadline has expired."
  )
  @Resolution("Retry the request with a greater deadline.")
  object RequestDeadlineExceeded
      extends ErrorCode(
        id = "REQUEST_DEADLINE_EXCEEDED",
        ErrorCategory.DeadlineExceededRequestStateUnknown,
      ) {
    final case class Reject(deadlineExceededBy: Duration, commandId: String, submissionId: String)(
        implicit loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The gRPC deadline for request with commandId=$commandId and submissionId=$submissionId has expired by $deadlineExceededBy. The request will not be processed further.",
          definiteAnswer = false,
        )
  }

  @Explanation(
    "This rejection is given when the requested service is not running. It has not started or has already been shut down."
  )
  @Resolution(
    "Retry re-submitting the request. If the error persists, contact the participant operator."
  )
  object ServiceNotRunning
      extends ErrorCode(
        id = "SERVICE_NOT_RUNNING",
        ErrorCategory.TransientServerFailure,
      ) {
    final case class Reject(serviceName: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"$serviceName is not running.",
          extraContext = Map("service_name" -> serviceName),
        )
  }

  @Explanation("This rejection is given when the participant server is shutting down.")
  @Resolution("Contact the participant operator.")
  object ServerIsShuttingDown
      extends ErrorCode(
        id = "SERVER_IS_SHUTTING_DOWN",
        ErrorCategory.TransientServerFailure,
      ) {
    final case class Reject()(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = "Server is shutting down"
        )
  }

  @Explanation("""This error occurs if one of the services encountered an unexpected exception.""")
  @Resolution("Contact support.")
  object ServiceInternalError
      extends ErrorCode(
        id = "SERVICE_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    final case class UnexpectedOrUnknownException(t: Throwable)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = "Unexpected or unknown exception occurred.",
          throwableO = Some(t),
        )

    final case class Generic(
        message: String,
        override val throwableO: Option[Throwable] = None,
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = message,
          extraContext = Map("throwableO" -> throwableO.toString),
        )
  }
}
