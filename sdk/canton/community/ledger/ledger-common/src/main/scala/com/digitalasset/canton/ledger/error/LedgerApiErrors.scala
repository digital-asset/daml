// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.error

import com.daml.error.*
import com.digitalasset.daml.lf.engine.Error.Validation.ReplayMismatch
import com.digitalasset.daml.lf.engine.Error as LfError
import com.daml.metrics.ExecutorServiceMetrics
import com.digitalasset.canton.ledger.error.ParticipantErrorGroup.LedgerApiErrorGroup
import org.slf4j.event.Level

@Explanation(
  "Errors raised by or forwarded by the Ledger API."
)
object LedgerApiErrors extends LedgerApiErrorGroup {

  val EarliestOffsetMetadataKey = "earliest_offset"
  val LatestOffsetMetadataKey = "latest_offset"

  @Explanation(
    """This error occurs when a participant rejects a command due to excessive load.
      |Load can be caused by the following factors:
      |1. when commands are submitted to the participant through its Ledger API,
      |2. when the participant receives validation requests from other participants through a connected domain.
      |
      |In order to prevent the participant of being overloaded, it will start to reject commands once a
      |certain load threshold is reached. The main threshold is the number of in-flight validation requests
      |that the participant is currently processing. These requests can be caused either by this participant
      |or by other participants.
      |
      |For a submission to be counted as an in-flight validation request, the participant must first
      |observe its sequencing, which means that there is a delay between the submission and the submitted
      |command to be counted towards the currently in-flight validation requests. In order to avoid an
      |overload situation by a sudden burst of commands, the participant will also enforce a rate limit
      |before a submission is accepted for interpretation. This rate limit can be configured with a steady
      |state rate and a burst factor. The burst factor is a multiplier of the steady state rate that allows
      |for a certain number of commands to be submitted in a burst before the rate limit kicks in.
      |
      |As an example, with a rate limit of 1000 commands per second and a burst factor of 2, the rate limit
      |will kick in once 2000 commands have been submitted on top of the commands allowed by the rate limit.
      |
      |"""
  )
  @Resolution(
    """Verify the limits configured, the load and the command latency on the participant and adjust if necessary.
      |If the participant is highly loaded, ensure that your application waits some time with the resubmission, preferably with some backoff factor.
      |If possible, ask other participants to send fewer requests; the domain operator can enforce this by imposing a rate limit."""
  )
  object ParticipantBackpressure
      extends ErrorCode(
        id = "PARTICIPANT_BACKPRESSURE",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    override def logLevel: Level = Level.INFO

    final case class Rejection(reason: String)(implicit errorLogger: ContextualizedErrorLogger)
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
    final case class Rejection(
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
    final case class Rejection(
        value: Long,
        limit: Int,
        metricPrefix: String,
        fullMethodName: String,
    )(implicit
        errorLogger: ContextualizedErrorLogger
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
      |Here the 'queue size' for the threadpool is considered as reported by the executor itself.
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
    final case class Rejection(
        name: String,
        metricNameLabel: String,
        queued: Long,
        limit: Int,
        fullMethodName: String,
    )(implicit errorLogger: ContextualizedErrorLogger)
        extends DamlErrorWithDefiniteAnswer(
          s"The $metricNameLabel ($name) queue size ($queued) has exceeded the maximum ($limit).",
          extraContext = Map(
            "name" -> name,
            "queued" -> queued,
            "limit" -> limit,
            "name_label" -> metricNameLabel,
            "metrics" -> ExecutorServiceMetrics.CommonMetricsName.QueuedTasks,
            "fullMethodName" -> fullMethodName,
          ),
        )
  }

  @Explanation("""This error occurs if there was an unexpected error in the Ledger API.""")
  @Resolution("Contact support.")
  object InternalError
      extends ErrorCode(
        id = "LEDGER_API_INTERNAL_ERROR",
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

    final case class PackageSelfConsistency(
        err: LfError.Package.SelfConsistency
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = err.message
        )

    final case class PackageInternal(
        err: LfError.Package.Internal
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = err.message
        )

    final case class Preprocessing(
        err: LfError.Preprocessing.Internal
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = err.message)

    final case class Validation(reason: ReplayMismatch)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Observed un-expected replay mismatch: $reason"
        )

    final case class Interpretation(
        where: String,
        message: String,
        detailMessage: Option[String],
    )(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(
          cause = s"Daml-Engine interpretation failed with internal error: $where / $message",
          extraContext = Map("detailMessage" -> detailMessage),
        )

    final case class VersionService(message: String)(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = message)

    final case class Buffer(message: String, override val throwableO: Option[Throwable])(implicit
        loggingContext: ContextualizedErrorLogger
    ) extends DamlErrorWithDefiniteAnswer(cause = message, throwableO = throwableO)
  }
}
