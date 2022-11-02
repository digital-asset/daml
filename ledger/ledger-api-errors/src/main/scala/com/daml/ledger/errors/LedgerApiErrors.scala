// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.errors

import com.daml.error.ErrorGroups.ParticipantErrorGroup.LedgerApiErrorGroup
import com.daml.error._
import com.daml.lf.engine.Error.Validation.ReplayMismatch
import com.daml.lf.engine.{Error => LfError}
import org.slf4j.event.Level

@Explanation(
  "Errors raised by or forwarded by the Ledger API."
)
object LedgerApiErrors extends LedgerApiErrorGroup {
  val Admin = groups.AdminServices
  val CommandExecution = groups.CommandExecution
  val AuthorizationChecks = groups.AuthorizationChecks
  val ConsistencyErrors = groups.ConsistencyErrors
  val RequestValidation = groups.RequestValidation
  val WriteServiceRejections = groups.WriteServiceRejections

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
          cause = s"The request exercised an unsupported operation: $message"
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
    "This error happens when the JVM heap memory pool exceeds a pre-configured limit."
  )
  @Resolution(
    """The following actions can be taken:
      |1. Review the historical use of heap space by inspecting the metric given in the message.
      |2. Review the current heap space limits configured in the rate limiting configuration.
      |3. Try to space out requests that are likely to require a large amount of memory to process."""
  )
  object HeapMemoryOverLimit
      extends ErrorCode(
        id = "HEAP_MEMORY_OVER_LIMIT",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    case class Rejection(
        memoryPool: String,
        limit: Long,
        metricPrefix: String,
        fullMethodName: String,
    )(implicit errorLogger: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The $memoryPool collection usage threshold has exceeded the maximum ($limit). Jvm memory metrics are available at $metricPrefix.",
          extraContext = Map(
            "memoryPool" -> memoryPool,
            "limit" -> limit,
            "metricPrefix" -> metricPrefix,
            "fullMethodName" -> fullMethodName,
          ),
        )
  }

  @Explanation(
    "This error happens when the number of concurrent gRPC streaming requests exceeds the configured limit."
  )
  @Resolution(
    """The following actions can be taken:
      |1. Review the historical need for concurrent streaming by inspecting the metric given in the message.
      |2. Review the maximum streams limit configured in the rate limiting configuration.
      |3. Try to space out streaming requests such that they do not need to run in parallel with each other."""
  )
  object MaximumNumberOfStreams
      extends ErrorCode(
        id = "MAXIMUM_NUMBER_OF_STREAMS",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    case class Rejection(value: Long, limit: Int, metricPrefix: String, fullMethodName: String)(
        implicit errorLogger: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause =
            s"The number of streams in use ($value) has reached or exceeded the limit ($limit). Metrics are available at $metricPrefix.",
          extraContext = Map(
            "value" -> value,
            "limit" -> limit,
            "metricPrefix" -> metricPrefix,
            "fullMethodName" -> fullMethodName,
          ),
        )
  }

  @Explanation(
    "This happens when the rate of submitted gRPC requests requires more CPU or database power than is available."
  )
  @Resolution(
    """The following actions can be taken:
      |Here the 'queue size' for the threadpool = 'submitted tasks' - 'completed tasks' - 'running tasks'
      |1. Review the historical 'queue size' growth by inspecting the metric given in the message.
      |2. Review the maximum 'queue size' limits configured in the rate limiting configuration.
      |3. Try to space out requests that are likely to require a lot of CPU or database power.
      """
  )
  object ThreadpoolOverloaded
      extends ErrorCode(
        id = "THREADPOOL_OVERLOADED",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    case class Rejection(
        name: String,
        queued: Long,
        limit: Int,
        metricPrefix: String,
        fullMethodName: String,
    )(implicit errorLogger: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          s"The $name queue size ($queued) has exceeded the maximum ($limit). Api services metrics are available at $metricPrefix.",
          extraContext = Map(
            "name" -> name,
            "queued" -> queued,
            "limit" -> limit,
            "metricPrefix" -> metricPrefix,
            "fullMethodName" -> fullMethodName,
          ),
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
          cause = s"$serviceName has been shut down.",
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
          cause = s"Observed un-expected replay mismatch: $reason"
        )

    case class Interpretation(
        where: String,
        message: String,
        detailMessage: Option[String],
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Daml-Engine interpretation failed with internal error: $where / $message",
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
