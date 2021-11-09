// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.sql.{SQLNonTransientException, SQLTransientException}
import java.time.Duration

import com.daml.error.ErrorCode.ApiException
import com.daml.error.definitions.{IndexErrors, LedgerApiErrors}
import com.daml.error.{ContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.lf.data.Ref.TransactionId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories.{
  addDefiniteAnswerDetails,
  definiteAnswers,
}
import com.daml.platform.server.api.{ValidationLogger, ApiException => NoStackTraceApiException}
import com.google.protobuf.{Any => AnyProto}
import com.google.rpc.{ErrorInfo, Status}
import io.grpc.Status.Code
import io.grpc.protobuf.StatusProto
import io.grpc.{Metadata, StatusRuntimeException}
import scalaz.syntax.tag._

class ErrorFactories private (errorCodesVersionSwitcher: ErrorCodesVersionSwitcher) {
  def sqlTransientException(exception: SQLTransientException)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    IndexErrors.DatabaseErrors.SqlTransientError.Reject(exception).asGrpcError

  def sqlNonTransientException(exception: SQLNonTransientException)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    IndexErrors.DatabaseErrors.SqlNonTransientError.Reject(exception).asGrpcError

  def transactionNotFound(transactionId: TransactionId)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = grpcError(
        Status
          .newBuilder()
          .setCode(Code.NOT_FOUND.value())
          .setMessage("Transaction not found, or not visible.")
          .build()
      ),
      v2 = LedgerApiErrors.ReadErrors.TransactionNotFound
        .Reject(transactionId)
        .asGrpcError,
    )

  def malformedPackageId[Request](request: Request, message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger,
      logger: ContextualizedLogger,
      loggingContext: LoggingContext,
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = ValidationLogger.logFailureWithContext(
        request,
        io.grpc.Status.INVALID_ARGUMENT
          .withDescription(message)
          .asRuntimeException(),
      ),
      v2 = LedgerApiErrors.ReadErrors.MalformedPackageId
        .Reject(
          message = message
        )
        .asGrpcError,
    )

  def packageNotFound(packageId: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = io.grpc.Status.NOT_FOUND.asRuntimeException(),
      v2 = LedgerApiErrors.ReadErrors.PackageNotFound.Reject(packageId = packageId).asGrpcError,
    )

  def versionServiceInternalError(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = io.grpc.Status.INTERNAL
        .withDescription(message)
        .asRuntimeException(),
      v2 = LedgerApiErrors.VersionServiceError.InternalError.Reject(message).asGrpcError,
    )

  def duplicateCommandException(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val exception = grpcError(
          Status
            .newBuilder()
            .setCode(Code.ALREADY_EXISTS.value())
            .setMessage("Duplicate command")
            .addDetails(definiteAnswers(false))
            .build()
        )
        contextualizedErrorLogger.info(exception.getMessage)
        exception
      },
      v2 = LedgerApiErrors.CommandPreparation.DuplicateCommand.Reject().asGrpcError,
    )

  /** @param expected Expected ledger id.
    * @param received Received ledger id.
    * @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.NOT_FOUND]] status code.
    */
  def ledgerIdMismatch(
      expected: LedgerId,
      received: LedgerId,
      definiteAnswer: Option[Boolean],
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusRuntimeException = {
    require(!definiteAnswer.contains(true), "Wrong ledger ID can never be a definite answer.")
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.NOT_FOUND.value())
          .setMessage(
            s"Ledger ID '${received.unwrap}' not found. Actual Ledger ID is '${expected.unwrap}'."
          )
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = LedgerApiErrors.CommandValidation.LedgerIdMismatch
        .Reject(
          s"Ledger ID '${received.unwrap}' not found. Actual Ledger ID is '${expected.unwrap}'."
        )
        .asGrpcError,
    )
  }

  /** @param fieldName A missing field's name.
    * @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def missingField(fieldName: String, definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.INVALID_ARGUMENT.value())
          .setMessage(s"Missing field: $fieldName")
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = LedgerApiErrors.CommandValidation.MissingField
        .Reject(fieldName)
        .asGrpcError,
    )

  /** @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @param message A status' message.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def invalidArgument(definiteAnswer: Option[Boolean])(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        invalidArgumentV1(definiteAnswer, message)
      },
      // TODO error codes: This error group is confusing for this generic error as it can be dispatched
      //                   from call-sites that do not involve command validation (e.g. ApiTransactionService).
      v2 = LedgerApiErrors.CommandValidation.InvalidArgument
        .Reject(message)
        .asGrpcError,
    )

  // This error builder covers cases where existing logic handling invalid arguments returned NOT_FOUND.
  def invalidArgumentWasNotFound(definiteAnswer: Option[Boolean])(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.NOT_FOUND.value())
          .setMessage(message)
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      // TODO error codes: Revisit this error: This error group is confusing for this generic error as it can be dispatched
      //                   from call-sites that do not involve command validation (e.g. ApiTransactionService, GrpcHealthService).
      v2 = LedgerApiErrors.CommandValidation.InvalidArgument
        .Reject(message)
        .asGrpcError,
    )

  // TODO error codes: Reconcile with com.daml.platform.server.api.validation.ErrorFactories.offsetOutOfRange
  def offsetOutOfRange_was_invalidArgument(
      definiteAnswer: Option[Boolean]
  )(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        invalidArgumentV1(definiteAnswer, message)
      },
      v2 = LedgerApiErrors.ReadErrors.RequestedOffsetOutOfRange
        .Reject(message)
        .asGrpcError,
    )

  def nonHexOffset(
      definiteAnswer: Option[Boolean]
  )(fieldName: String, offsetValue: String, message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = invalidArgumentV1(definiteAnswer, message),
      v2 = LedgerApiErrors.NonHexOffset
        .Error(
          fieldName = fieldName,
          offsetValue = offsetValue,
          message = message,
        )
        .asGrpcError,
    )

  def invalidDeduplicationDuration(
      fieldName: String,
      message: String,
      definiteAnswer: Option[Boolean],
      maxDeduplicationDuration: Duration,
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      legacyInvalidField(fieldName, message, definiteAnswer),
      LedgerApiErrors.CommandValidation.InvalidDeduplicationPeriodField
        .Reject(message, maxDeduplicationDuration)
        .asGrpcError,
    )

  /** @param fieldName An invalid field's name.
    * @param message A status' message.
    * @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def invalidField(
      fieldName: String,
      message: String,
      definiteAnswer: Option[Boolean],
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = legacyInvalidField(fieldName, message, definiteAnswer),
      v2 = ledgerCommandValidationInvalidField(fieldName, message).asGrpcError,
    )

  private def ledgerCommandValidationInvalidField(fieldName: String, message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): LedgerApiErrors.CommandValidation.InvalidField.Reject = {
    LedgerApiErrors.CommandValidation.InvalidField
      .Reject(s"Invalid field $fieldName: $message")
  }

  private def legacyInvalidField(
      fieldName: String,
      message: String,
      definiteAnswer: Option[Boolean],
  ): StatusRuntimeException = {
    val statusBuilder = Status
      .newBuilder()
      .setCode(Code.INVALID_ARGUMENT.value())
      .setMessage(s"Invalid field $fieldName: $message")
    addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
    grpcError(statusBuilder.build())
  }

  def offsetOutOfRange(description: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    // TODO error codes: Pass the offsets as arguments to this method and build the description here
    errorCodesVersionSwitcher.choose(
      v1 = grpcError(
        Status
          .newBuilder()
          .setCode(Code.OUT_OF_RANGE.value())
          .setMessage(description)
          .build()
      ),
      v2 = LedgerApiErrors.ReadErrors.RequestedOffsetOutOfRange.Reject(description).asGrpcError,
    )

  /** @param message A status' message.
    * @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.ABORTED]] status code.
    */
  def aborted(message: String, definiteAnswer: Option[Boolean]): StatusRuntimeException = {
    // TODO error codes: This error code is not specific enough.
    //                   Break down into more specific errors.
    val statusBuilder = Status
      .newBuilder()
      .setCode(Code.ABORTED.value())
      .setMessage(message)
    addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
    grpcError(statusBuilder.build())
  }

  def isTimeoutUnknown_wasAborted(message: String, definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    errorCodesVersionSwitcher.choose(
      v1 = aborted(message, definiteAnswer),
      v2 = LedgerApiErrors.WriteErrors.RequestTimeOut
        .Reject(
          message,
          // TODO error codes: How to handle None definiteAnswer?
          definiteAnswer.getOrElse(false),
        )
        .asGrpcError,
    )
  }

  def packageUploadRejected(message: String, definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    errorCodesVersionSwitcher.choose(
      v1 = invalidArgumentV1(definiteAnswer, message),
      v2 = LedgerApiErrors.WriteErrors.PackageUploadRejected.Reject(message).asGrpcError,
    )
  }

  def configurationEntryRejected(message: String, definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    errorCodesVersionSwitcher.choose(
      v1 = aborted(message, definiteAnswer),
      v2 = LedgerApiErrors.WriteErrors.ConfigurationEntryRejected.Reject(message).asGrpcError,
    )
  }

  // permission denied is intentionally without description to ensure we don't leak security relevant information by accident
  def permissionDenied(cause: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = errorCodesVersionSwitcher.choose(
    v1 = {
      contextualizedErrorLogger.warn(s"Permission denied. Reason: $cause.")
      new ApiException(
        io.grpc.Status.PERMISSION_DENIED,
        new Metadata(),
      )
    },
    v2 = LedgerApiErrors.AuthorizationChecks.PermissionDenied.Reject(cause).asGrpcError,
  )

  def unauthenticatedMissingJwtToken()(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = errorCodesVersionSwitcher.choose(
    v1 = new ApiException(
      io.grpc.Status.UNAUTHENTICATED,
      new Metadata(),
    ),
    v2 = LedgerApiErrors.AuthorizationChecks.Unauthenticated
      .MissingJwtToken()
      .asGrpcError,
  )

  def internalAuthenticationError(securitySafeMessage: String, exception: Throwable)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        contextualizedErrorLogger.warn(
          s"$securitySafeMessage: ${exception.getMessage}"
        )
        new ApiException(
          io.grpc.Status.INTERNAL.withDescription(securitySafeMessage),
          new Metadata(),
        )
      },
      v2 = LedgerApiErrors.AuthorizationChecks.InternalAuthorizationError
        .Reject(securitySafeMessage, exception)
        .asGrpcError,
    )

  /** @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.UNAVAILABLE]] status code.
    */
  def missingLedgerConfig(definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.UNAVAILABLE.value())
          .setMessage("The ledger configuration is not available.")
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      // TODO error codes: This error group is confusing for this generic error as it can be dispatched
      //                   from call-sites that do not involve Daml interpreter.
      v2 = LedgerApiErrors.InterpreterErrors.LookupErrors.LedgerConfigurationNotFound
        .Reject()
        .asGrpcError,
    )

  // TODO error codes: Duplicate of missingLedgerConfig
  def missingLedgerConfigUponRequest(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = errorCodesVersionSwitcher.choose(
    v1 = grpcError(
      Status
        .newBuilder()
        .setCode(Code.NOT_FOUND.value())
        .setMessage("The ledger configuration is not available.")
        .build()
    ),
    v2 = LedgerApiErrors.InterpreterErrors.LookupErrors.LedgerConfigurationNotFound
      .Reject()
      .asGrpcError,
  )

  def participantPrunedDataAccessed(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = grpcError(
        Status
          .newBuilder()
          .setCode(Code.NOT_FOUND.value())
          .setMessage(message)
          .build()
      ),
      v2 = LedgerApiErrors.ReadErrors.ParticipantPrunedDataAccessed.Reject(message).asGrpcError,
    )

  /** @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.UNAVAILABLE]] status code.
    */
  def serviceNotRunning(definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.UNAVAILABLE.value())
          .setMessage("Service has been shut down.")
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      // TODO error codes: Add service name to the error cause
      v2 = LedgerApiErrors.ServiceNotRunning.Reject().asGrpcError,
    )

  def trackerFailure(msg: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val builder = Status
          .newBuilder()
          .setCode(Code.INTERNAL.value())
          .setMessage(msg)
        grpcError(builder.build())
      },
      v2 = LedgerApiErrors.InternalError.CommandTrackerInternalError(msg).asGrpcError,
    )

  /** Transforms Protobuf [[Status]] objects, possibly including metadata packed as [[ErrorInfo]] objects,
    * into exceptions with metadata in the trailers.
    *
    * Asynchronous errors, i.e. failed completions, contain Protobuf [[Status]] objects themselves.
    *
    * @param status A Protobuf [[Status]] object.
    * @return An exception without a stack trace.
    */
  def grpcError(status: Status): StatusRuntimeException = new NoStackTraceApiException(
    StatusProto.toStatusRuntimeException(status)
  )

  private def invalidArgumentV1(
      definiteAnswer: Option[Boolean],
      message: String,
  ): StatusRuntimeException = {
    val statusBuilder = Status
      .newBuilder()
      .setCode(Code.INVALID_ARGUMENT.value())
      .setMessage(s"Invalid argument: $message")
    addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
    grpcError(statusBuilder.build())
  }
}

object ErrorFactories {
  val SelfServiceErrorCodeFactories: ErrorFactories = ErrorFactories(
    new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = true)
  )

  def apply(errorCodesVersionSwitcher: ErrorCodesVersionSwitcher): ErrorFactories =
    new ErrorFactories(errorCodesVersionSwitcher)

  private[daml] lazy val definiteAnswers = Map(
    true -> AnyProto.pack[ErrorInfo](
      ErrorInfo.newBuilder().putMetadata(GrpcStatuses.DefiniteAnswerKey, "true").build()
    ),
    false -> AnyProto.pack[ErrorInfo](
      ErrorInfo.newBuilder().putMetadata(GrpcStatuses.DefiniteAnswerKey, "false").build()
    ),
  )

  private def addDefiniteAnswerDetails(
      definiteAnswer: Option[Boolean],
      statusBuilder: Status.Builder,
  ): Unit = {
    definiteAnswer.foreach { definiteAnswer =>
      statusBuilder.addDetails(definiteAnswers(definiteAnswer))
    }
  }
}
