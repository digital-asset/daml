// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import io.grpc.Status.Code
import org.slf4j.event.Level

import scala.concurrent.duration._

/** Standard error categories
  *
  * Ideally, all products will return errors with appropriate error codes. Every such
  * error code is associated with an error category that defines how the error will appear
  * in the log file and on the api level.
  */
sealed trait ErrorCategory {

  /** The Grpc code use to signal this error (in case it is signalled via API) */
  def grpcCode: Option[Code]

  /** The log level used to log this error on the server side */
  def logLevel: Level

  /** Default retryability information for this error category */
  def retryable: Option[ErrorCategoryRetry]

  /** If true, the event is security sensitive and error details should not be emitted on the api */
  def securitySensitive: Boolean

  /** Int representation of this error category */
  def asInt: Int

  /** Rank used to order severity (internal only) */
  def rank: Int
}

object ErrorCategory {

  val all: Seq[ErrorCategory] =
    Seq(
      TransientServerFailure,
      ContentionOnSharedResources,
      DeadlineExceededRequestStateUnknown,
      SystemInternalAssumptionViolated,
      MaliciousOrFaultyBehaviour,
      AuthInterceptorInvalidAuthenticationCredentials,
      InsufficientPermission,
      InvalidIndependentOfSystemState,
      InvalidGivenCurrentSystemStateOther,
      InvalidGivenCurrentSystemStateResourceExists,
      InvalidGivenCurrentSystemStateResourceMissing,
      InvalidGivenCurrentSystemStateSeekAfterEnd,
      BackgroundProcessDegradationWarning,
      InternalUnsupportedOperation,
    )

  def fromInt(ii: Int): Option[ErrorCategory] = all.find(_.asInt == ii)

  abstract class ErrorCategoryImpl(
      val grpcCode: Option[Code],
      val logLevel: Level,
      val retryable: Option[ErrorCategoryRetry],
      val securitySensitive: Boolean,
      val asInt: Int,
      val rank: Int,
  )

  /** Service is temporarily unavailable
    */
  @Description("One of the services required to process the request was not available.")
  @RetryStrategy("Retry quickly in load balancer.")
  @Resolution(
    "Expectation: transient failure that should be handled by retrying the request with appropriate backoff."
  )
  object TransientServerFailure
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.UNAVAILABLE),
        logLevel = Level.INFO,
        retryable = Some(ErrorCategoryRetry(1.second)),
        securitySensitive = false,
        asInt = 1,
        rank = 3,
      )
      with ErrorCategory

  /** Failure due to contention on some resources
    */
  @Description(
    """The request could not be processed due to shared processing resources
                 |(e.g. locks or rate limits that replenish quickly) being occupied.
                 |If the resource is known (i.e. locked contract), it will be included as a resource info. (Not known
                 |resource contentions are e.g. overloaded networks where we just observe timeouts, but can’t pin-point the cause)."""
  )
  @RetryStrategy("Retry quickly (indefinitely or limited), but do not retry in load balancer.")
  @Resolution("""Expectation: this is processing-flow level contention that should be handled by
                |retrying the request with appropriate backoff.""")
  object ContentionOnSharedResources
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.ABORTED),
        logLevel = Level.INFO,
        retryable = Some(ErrorCategoryRetry(1.second)),
        securitySensitive = false,
        asInt = 2,
        rank = 3,
      )
      with ErrorCategory

  /** Request completion not observed within a pre-defined window
    */
  @Description("""The request might not have been processed, as its deadline expired before its
                 |completion was signalled. Note that for requests that change the state of the
                 |system, this error may be returned even if the request has completed successfully.
                 |Note that known and well-defined timeouts are signalled as
                 |[[ContentionOnSharedResources]], while this category indicates that the
                 |state of the request is unknown.""")
  @RetryStrategy("Retry for a limited number of times with deduplication.")
  @Resolution(
    """Expectation: the deadline might have been exceeded due to transient resource
                |congestion or due to a timeout in the request processing pipeline being too low.
                |The transient errors might be solved by the application retrying.
                |The non-transient errors will require operator intervention to change the timeouts."""
  )
  object DeadlineExceededRequestStateUnknown
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.DEADLINE_EXCEEDED),
        logLevel = Level.INFO,
        retryable = Some(ErrorCategoryRetry(1.second)),
        securitySensitive = false,
        asInt = 3,
        rank = 3,
      )
      with ErrorCategory

  /** Some internal error
    */
  @Description("Request processing failed due to a violation of system internal invariants.")
  @RetryStrategy("Retry after operator intervention.")
  @Resolution(
    """Expectation: this is due to a bug in the implementation or data corruption in the systems databases.
                |Resolution will require operator intervention, and potentially vendor support."""
  )
  object SystemInternalAssumptionViolated
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.INTERNAL),
        logLevel = Level.ERROR,
        retryable = None,
        securitySensitive = true,
        asInt = 4,
        rank = 1,
      )
      with ErrorCategory

  /** Malicious or faulty behaviour detected
    */
  @Description("""Request processing failed due to unrecoverable data loss or corruption
                 |(e.g. detected via checksums)""")
  @RetryStrategy("Retry after operator intervention.")
  @Resolution(
    """Expectation: this can be a severe issue that requires operator attention or intervention, and
                |potentially vendor support."""
  )
  object MaliciousOrFaultyBehaviour
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.UNKNOWN),
        logLevel = Level.WARN,
        retryable = None,
        securitySensitive = true,
        asInt = 5,
        rank = 2,
      )
      with ErrorCategory

  /** Client is not authenticated properly
    */
  @Description("""The request does not have valid authentication credentials for the operation.""")
  @RetryStrategy("""Retry after application operator intervention.""")
  @Resolution(
    """Expectation: this is an application bug, application misconfiguration or ledger-level
                |misconfiguration. Resolution requires application and/or ledger operator intervention."""
  )
  object AuthInterceptorInvalidAuthenticationCredentials
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.UNAUTHENTICATED),
        logLevel = Level.WARN,
        retryable = None,
        securitySensitive = true,
        asInt = 6,
        rank = 2,
      )
      with ErrorCategory

  /** Client does not have appropriate permissions
    */
  @Description("""The caller does not have permission to execute the specified operation.""")
  @RetryStrategy("""Retry after application operator intervention.""")
  @Resolution(
    """Expectation: this is an application bug or application misconfiguration. Resolution requires
                |application operator intervention."""
  )
  object InsufficientPermission
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.PERMISSION_DENIED),
        logLevel = Level.WARN,
        retryable = None,
        securitySensitive = true,
        asInt = 7,
        rank = 2,
      )
      with ErrorCategory

  /** A request which is never going to be valid
    */
  @Description("""The request is invalid independent of the state of the system.""")
  @RetryStrategy("""Retry after application operator intervention.""")
  @Resolution(
    """Expectation: this is an application bug or ledger-level misconfiguration (e.g. request size limits).
                |Resolution requires application and/or ledger operator intervention."""
  )
  object InvalidIndependentOfSystemState
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.INVALID_ARGUMENT),
        logLevel = Level.INFO,
        retryable = None,
        securitySensitive = false,
        asInt = 8,
        rank = 3,
      )
      with ErrorCategory

  /** A failure due to the current system state
    */
  @Description(
    """The mutable state of the system does not satisfy the preconditions required to execute the request.
                 |We consider the whole Daml ledger including ledger config, parties, packages, users and command
                 |deduplication to be mutable system state. Thus all Daml interpretation errors are reported
                 |as this error or one of its specializations."""
  )
  @RetryStrategy("""Retry after application operator intervention.""")
  @Resolution("""ALREADY_EXISTS and NOT_FOUND are special cases for the existence and non-existence of well-defined
                |entities within the system state; e.g., a .dalf package, contracts ids, contract keys, or a
                |transaction at an offset. OUT_OF_RANGE is a special case for reading past a range. Violations of the
                |Daml ledger model always result in these kinds of errors. Expectation: this is due to
                |application-level bugs, misconfiguration or contention on application-visible resources; and might be
                |resolved by retrying later, or after changing the state of the system. Handling these errors requires
                |an application-specific strategy and/or operator intervention.""")
  object InvalidGivenCurrentSystemStateOther
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.FAILED_PRECONDITION),
        logLevel = Level.INFO,
        retryable = None,
        securitySensitive = false,
        asInt = 9,
        rank = 3,
      )
      with ErrorCategory

  /** A failure due to a resource already existing in the current system state
    */
  @Description("""Special type of InvalidGivenCurrentSystemState referring to a well-defined
                 |resource.""")
  @RetryStrategy(
    """Inspect resource failure and retry after resource failure has been resolved (depends on type of
                   |resource and application)."""
  )
  @Resolution("""Same as [[InvalidGivenCurrentSystemStateOther]].""")
  object InvalidGivenCurrentSystemStateResourceExists
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.ALREADY_EXISTS),
        logLevel = Level.INFO,
        retryable = None,
        securitySensitive = false,
        asInt = 10,
        rank = 3,
      )
      with ErrorCategory

  /** A failure due to a resource not existing in the current system state
    */
  @Description("""Special type of InvalidGivenCurrentSystemState referring to a well-defined
                 |resource.""")
  @RetryStrategy(
    """Inspect resource failure and retry after resource failure has been resolved (depends on type of
                   |resource and application)."""
  )
  @Resolution("""Same as [[InvalidGivenCurrentSystemStateOther]].""")
  object InvalidGivenCurrentSystemStateResourceMissing
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.NOT_FOUND),
        logLevel = Level.INFO,
        retryable = None,
        securitySensitive = false,
        asInt = 11,
        rank = 3,
      )
      with ErrorCategory

  /** The supplied offset is out of range
    */
  @Description(
    """This error is only used by the Ledger API server in connection with invalid offsets."""
  )
  @RetryStrategy("""Retry after application operator intervention.""")
  @Resolution(
    """Expectation: this error is only used by the Ledger API server in connection with invalid offsets."""
  )
  object InvalidGivenCurrentSystemStateSeekAfterEnd
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.OUT_OF_RANGE),
        logLevel = Level.INFO,
        retryable = None,
        securitySensitive = false,
        asInt = 12,
        rank = 3,
      )
      with ErrorCategory

  /** Background daemon notifying about observed degradation
    */
  @Description(
    """This error category is used internally to signal to the system operator an internal degradation."""
  )
  @RetryStrategy("""Not an API error, therefore not retryable.""")
  @Resolution("""Inspect details of the specific error for more information.""")
  object BackgroundProcessDegradationWarning
      extends ErrorCategoryImpl(
        grpcCode = None, // should not be used on the API level
        logLevel = Level.WARN,
        retryable = None,
        securitySensitive = false,
        asInt = 13,
        rank = 2,
      )
      with ErrorCategory

  @Description(
    """This error category is used to signal that an unimplemented code-path has been triggered by a client or participant operator request."""
  )
  @RetryStrategy("""Errors in this category are non-retryable.""")
  @Resolution(
    """This error is caused by a ledger-level misconfiguration or by an implementation bug.
      |Resolution requires participant operator intervention."""
  )
  object InternalUnsupportedOperation
      extends ErrorCategoryImpl(
        grpcCode = Some(Code.UNIMPLEMENTED),
        logLevel = Level.ERROR,
        retryable = None,
        securitySensitive = true,
        asInt = 14,
        rank = 1,
      )
      with ErrorCategory

  implicit val orderingErrorType: Ordering[ErrorCategory] = Ordering.by[ErrorCategory, Int](_.rank)
}

/** Default retryability information
  *
  * Every error category has a default retryability classification.
  * An error code may adjust the retry duration.
  */
case class ErrorCategoryRetry(duration: Duration)
