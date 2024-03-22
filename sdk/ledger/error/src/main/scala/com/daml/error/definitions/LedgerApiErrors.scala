// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.definitions

import com.daml.error._
import com.daml.error.definitions.ErrorGroups.ParticipantErrorGroup.LedgerApiErrorGroup
import com.daml.lf.engine.Error.Validation.ReplayMismatch
import com.daml.lf.engine.{Error => LfError}
import org.slf4j.event.Level

@Explanation(
  "Errors raised by or forwarded by the Ledger API."
)
object LedgerApiErrors extends LedgerApiErrorGroup {

  val Admin: groups.AdminServices.type = groups.AdminServices
  val CommandExecution: groups.CommandExecution.type = groups.CommandExecution
  val AuthorizationChecks: groups.AuthorizationChecks.type = groups.AuthorizationChecks
  val ConsistencyErrors: groups.ConsistencyErrors.type = groups.ConsistencyErrors
  val RequestValidation: groups.RequestValidation.type = groups.RequestValidation
  val WriteServiceRejections: groups.WriteServiceRejections.type = groups.WriteServiceRejections

  val EarliestOffsetMetadataKey = "earliest_offset"

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

    case class Reject(message: String)(implicit errorLogger: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          cause = s"The request exercised an unsupported operation: ${message}"
        )
  }

  @Explanation(
    """This error occurs when a participant rejects a command due to excessive load.
        |Load can be caused by the following factors:
        |1. when commands are submitted to the participant through its Ledger API,
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
    override def logLevel: Level = Level.INFO

    case class Rejection(reason: String)(implicit errorLogger: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          cause = s"The participant is overloaded: $reason",
          extraContext = Map("reason" -> reason),
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
    case class Reject(message: String, override val definiteAnswer: Boolean)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = message,
          definiteAnswer = definiteAnswer,
        )
  }

  @Explanation("This rejection is given when the requested service has already been closed.")
  @Resolution(
    "Retry re-submitting the request. If the error persists, contact the participant operator."
  )
  object ServiceNotRunning
      extends ErrorCode(
        id = "SERVICE_NOT_RUNNING",
        ErrorCategory.TransientServerFailure,
      ) {
    case class Reject(serviceName: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"${serviceName} has been shut down.",
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
    case class Reject()(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = "Server is shutting down"
        )
  }

  @Explanation("""This error occurs if there was an unexpected error in the Ledger API.""")
  @Resolution("Contact support.")
  object InternalError
      extends ErrorCode(
        id = "LEDGER_API_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {

    case class UnexpectedOrUnknownException(t: Throwable)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = "Unexpected or unknown exception occurred.",
          throwableO = Some(t),
        )

    case class Generic(
        message: String,
        override val throwableO: Option[Throwable] = None,
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = message,
          extraContext = Map("throwableO" -> throwableO.toString),
        )

    case class PackageSelfConsistency(
        err: LfError.Package.SelfConsistency
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = err.message
        )

    case class PackageInternal(
        err: LfError.Package.Internal
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = err.message
        )

    case class Preprocessing(
        err: LfError.Preprocessing.Internal
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = err.message)

    case class Validation(reason: ReplayMismatch)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Observed un-expected replay mismatch: ${reason}"
        )

    case class Interpretation(
        where: String,
        message: String,
        detailMessage: Option[String],
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Daml-Engine interpretation failed with internal error: ${where} / ${message}",
          extraContext = Map("detailMessage" -> detailMessage),
        )

    case class VersionService(message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = message)

    case class Buffer(message: String, override val throwableO: Option[Throwable])(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = message, throwableO = throwableO)
  }

}
