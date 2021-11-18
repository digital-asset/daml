// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import com.daml.error.ErrorCode.ApiException
import com.daml.error.definitions.{IndexErrors, LedgerApiErrors}
import com.daml.error.{ContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.lf.data.Ref.TransactionId
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.platform.server.api.validation.ErrorFactories.{
  addDefiniteAnswerDetails,
  definiteAnswers,
}
import com.daml.platform.server.api.{ApiException => NoStackTraceApiException}
import com.google.protobuf.{Any => AnyProto}
import com.google.rpc.status.{Status => RpcStatus}
import com.google.rpc.{ErrorInfo, Status}
import io.grpc.Status.Code
import io.grpc.protobuf.StatusProto
import io.grpc.{Metadata, StatusRuntimeException}
import scalaz.syntax.tag._

import java.sql.{SQLNonTransientException, SQLTransientException}
import java.time.{Duration, Instant}
import scala.annotation.nowarn

class ErrorFactories private (errorCodesVersionSwitcher: ErrorCodesVersionSwitcher) {
  object SubmissionQueueErrors {
    def failedToEnqueueCommandSubmission(message: String)(t: Throwable)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Status =
      errorCodesVersionSwitcher.choose(
        v1 = {
          val status = io.grpc.Status.ABORTED
            .withDescription(s"$message: ${t.getClass.getSimpleName}: ${t.getMessage}")
            .withCause(t)
          val statusBuilder = GrpcStatus.toJavaBuilder(status)
          GrpcStatus.buildStatus(Map.empty, statusBuilder)
        },
        v2 = LedgerApiErrors.InternalError
          .CommandTrackerInternalError(
            message = s"$message: ${t.getClass.getSimpleName}: ${t.getMessage}",
            throwableO = Some(t),
          )
          .asGrpcStatusFromContext,
      )

    def queueClosed(queueName: String)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Status =
      errorCodesVersionSwitcher.choose(
        v1 = {
          val status = io.grpc.Status.ABORTED.withDescription("Queue closed")
          val statusBuilder = GrpcStatus.toJavaBuilder(status)
          GrpcStatus.buildStatus(Map.empty, statusBuilder)
        },
        v2 = LedgerApiErrors.ServiceNotRunning
          .Reject(queueName)
          .asGrpcStatusFromContext,
      )

    def timedOutOnAwaitingForCommandCompletion()(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Status =
      errorCodesVersionSwitcher.choose(
        v1 = {
          val statusBuilder =
            GrpcStatus.toJavaBuilder(Code.ABORTED.value(), Some("Timeout"), Iterable.empty)
          GrpcStatus.buildStatus(Map.empty, statusBuilder)
        },
        v2 = LedgerApiErrors.RequestTimeOut
          .Reject(
            "Timed out while awaiting for a completion corresponding to a command submission.",
            definiteAnswer = false,
          )
          .asGrpcStatusFromContext,
      )

    def noStatusInCompletionResponse()(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Status = {
      errorCodesVersionSwitcher.choose(
        v1 = {
          Status
            .newBuilder()
            .setCode(Code.INTERNAL.value())
            .setMessage("Missing status in completion response.")
            .build()
        },
        v2 = LedgerApiErrors.InternalError
          .CommandTrackerInternalError(
            "Missing status in completion response.",
            throwableO = None,
          )
          .asGrpcStatusFromContext,
      )
    }
  }

  def bufferFull(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Status =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val status = io.grpc.Status.RESOURCE_EXHAUSTED
          .withDescription("Ingress buffer is full")
        val statusBuilder = GrpcStatus.toJavaBuilder(status)
        GrpcStatus.buildStatus(Map.empty, statusBuilder)
      },
      v2 = LedgerApiErrors.ParticipantBackpressure
        .Rejection(message)
        .asGrpcStatusFromContext,
    )

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
      v2 = LedgerApiErrors.RequestValidation.NotFound.Transaction
        .Reject(transactionId)
        .asGrpcError,
    )

  def packageNotFound(packageId: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = io.grpc.Status.NOT_FOUND.asRuntimeException(),
      v2 = LedgerApiErrors.RequestValidation.NotFound.Package
        .Reject(packageId = packageId)
        .asGrpcError,
    )

  def versionServiceInternalError(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = grpcError(
        Status
          .newBuilder()
          .setCode(Code.INTERNAL.value())
          .setMessage(message)
          .build()
      ),
      v2 = LedgerApiErrors.InternalError.VersionService(message).asGrpcError,
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
      v2 = LedgerApiErrors.ConsistencyErrors.DuplicateCommand.Reject().asGrpcError,
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
      v2 = LedgerApiErrors.RequestValidation.LedgerIdMismatch
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
      v2 = LedgerApiErrors.RequestValidation.MissingField
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
      v1 = invalidArgumentV1(definiteAnswer, message),
      v2 = LedgerApiErrors.RequestValidation.InvalidArgument
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
      v2 = LedgerApiErrors.RequestValidation.InvalidArgument
        .Reject(message)
        .asGrpcError,
    )

  def offsetOutOfRange(
      definiteAnswer: Option[Boolean]
  )(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = invalidArgumentV1(definiteAnswer, message),
      v2 = LedgerApiErrors.RequestValidation.OffsetOutOfRange
        .Reject(message)
        .asGrpcError,
    )

  def offsetAfterLedgerEnd(offsetType: String, requestedOffset: String, ledgerEnd: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    errorCodesVersionSwitcher.choose(
      v1 = grpcError(
        Status
          .newBuilder()
          .setCode(Code.OUT_OF_RANGE.value())
          .setMessage(s"$offsetType offset ($requestedOffset) is after ledger end ($ledgerEnd)")
          .build()
      ),
      v2 = LedgerApiErrors.RequestValidation.OffsetAfterLedgerEnd
        .Reject(offsetType, requestedOffset, ledgerEnd)
        .asGrpcError,
    )
  }

  def nonHexOffset(
      definiteAnswer: Option[Boolean]
  )(fieldName: String, offsetValue: String, message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = invalidArgumentV1(definiteAnswer, message),
      v2 = LedgerApiErrors.RequestValidation.NonHexOffset
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
      LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField
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
      v2 = ledgerRequestValidationInvalidField(fieldName, message).asGrpcError,
    )

  private def ledgerRequestValidationInvalidField(fieldName: String, message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): LedgerApiErrors.RequestValidation.InvalidField.Reject = {
    LedgerApiErrors.RequestValidation.InvalidField
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

  /** @param message A status' message.
    * @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.ABORTED]] status code.
    */
  @deprecated
  def aborted(message: String, definiteAnswer: Option[Boolean]): StatusRuntimeException = {
    val statusBuilder = Status
      .newBuilder()
      .setCode(Code.ABORTED.value())
      .setMessage(message)
    addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
    grpcError(statusBuilder.build())
  }

  @nowarn("msg=deprecated")
  def isTimeoutUnknown_wasAborted(message: String, definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    errorCodesVersionSwitcher.choose(
      v1 = aborted(message, definiteAnswer),
      v2 = LedgerApiErrors.RequestTimeOut
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
      v2 = LedgerApiErrors.AdminServices.PackageUploadRejected.Reject(message).asGrpcError,
    )
  }

  @nowarn("msg=deprecated")
  def configurationEntryRejected(message: String, definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException = {
    errorCodesVersionSwitcher.choose(
      v1 = aborted(message, definiteAnswer),
      v2 = LedgerApiErrors.AdminServices.ConfigurationEntryRejected.Reject(message).asGrpcError,
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
          io.grpc.Status.INTERNAL.withDescription(truncated(securitySafeMessage)),
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
  def missingLedgerConfig(
      legacyGrpcStatusCode: io.grpc.Status.Code
  )(definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(legacyGrpcStatusCode.value())
          .setMessage("The ledger configuration is not available.")
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = LedgerApiErrors.RequestValidation.NotFound.LedgerConfiguration
        .Reject()
        .asGrpcError,
    )

  def missingLedgerConfig(
      v1Status: RpcStatus,
      message: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): com.google.rpc.status.Status =
    errorCodesVersionSwitcher.choose(
      v1 = v1Status,
      v2 = GrpcStatus.toProto(
        LedgerApiErrors.RequestValidation.NotFound.LedgerConfiguration
          .RejectWithMessage(message)
          .asGrpcStatusFromContext
      ),
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
      v2 = LedgerApiErrors.RequestValidation.ParticipantPrunedDataAccessed
        .Reject(message)
        .asGrpcError,
    )

  /** @param definiteAnswer A flag that says whether it is a definite answer. Provided only in the context of command deduplication.
    * @return An exception with the [[Code.UNAVAILABLE]] status code.
    */
  def serviceNotRunning(serviceName: String)(definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(Code.UNAVAILABLE.value())
          .setMessage(s"$serviceName has been shut down.")
        addDefiniteAnswerDetails(definiteAnswer, statusBuilder)
        grpcError(statusBuilder.build())
      },
      v2 = LedgerApiErrors.ServiceNotRunning.Reject(serviceName).asGrpcError,
    )

  def serviceIsBeingReset(legacyStatusCode: Int)(serviceName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    errorCodesVersionSwitcher.choose(
      v1 = {
        val statusBuilder = Status
          .newBuilder()
          .setCode(legacyStatusCode)
          .setMessage(s"$serviceName is currently being reset.")
        grpcError(statusBuilder.build())
      },
      v2 = LedgerApiErrors.ServiceNotRunning.ServiceReset(serviceName).asGrpcError,
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

  /** Transforms Protobuf [[Status]] objects, possibly including metadata packed as [[ErrorInfo]] objects,
    * into exceptions with metadata in the trailers.
    *
    * Asynchronous errors, i.e. failed completions, contain Protobuf [[Status]] objects themselves.
    *
    * NOTE: The length of the Status message is truncated to a reasonable size for satisfying
    *        the Netty header size limit - as the message is also incorporated in the header, bundled in the gRPC metadata.
    * @param status A Protobuf [[Status]] object.
    * @return An exception without a stack trace.
    */
  def grpcError(status: Status): StatusRuntimeException = {
    val newStatus =
      Status
        .newBuilder(status)
        .setMessage(truncated(status.getMessage))
    new NoStackTraceApiException(
      StatusProto.toStatusRuntimeException(newStatus.build)
    )
  }

  private def truncated(message: String): String = {
    val maxMessageLength =
      1536 // An arbitrary limit that doesn't break netty serialization while being useful to human operator.
    if (message.length > maxMessageLength) message.take(maxMessageLength) + "..." else message
  }

  object CommandRejections {
    @deprecated
    def partyNotKnownOnLedger(reason: String)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      errorCodesVersionSwitcher.choose(
        v1 = RpcStatus
          .of(Code.INVALID_ARGUMENT.value(), s"Parties not known on ledger: $reason", Seq.empty),
        v2 = GrpcStatus.toProto(
          LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
            .RejectDeprecated(reason)
            .asGrpcStatusFromContext
        ),
      )

    def contractsNotFound(missingContractIds: Set[String])(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      errorCodesVersionSwitcher.choose(
        v1 = RpcStatus.of(
          Code.ABORTED.value(),
          s"Inconsistent: Could not lookup contracts: ${missingContractIds.mkString("[", ", ", "]")}",
          Seq.empty,
        ),
        v2 = GrpcStatus.toProto(
          LedgerApiErrors.ConsistencyErrors.ContractNotFound
            .MultipleContractsNotFound(missingContractIds)
            .asGrpcStatusFromContext
        ),
      )

    def inconsistentContractKeys(
        lookupResult: Option[Value.ContractId],
        currentResult: Option[Value.ContractId],
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      errorCodesVersionSwitcher.choose(
        v1 = RpcStatus.of(
          Code.ABORTED.value(),
          s"Inconsistent: Contract key lookup with different results: expected [$lookupResult], actual [$currentResult]",
          Seq.empty,
        ),
        v2 = GrpcStatus.toProto(
          LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
            .Reject(
              s"Contract key lookup with different results: expected [$lookupResult], actual [$currentResult]"
            )
            .asGrpcStatusFromContext
        ),
      )

    def duplicateContractKey(reason: String, key: GlobalKey)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      errorCodesVersionSwitcher.choose(
        v1 = RpcStatus.of(Code.ABORTED.value(), s"Inconsistent: $reason", Seq.empty),
        v2 = GrpcStatus.toProto(
          LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
            .RejectWithContractKeyArg(reason, key)
            .asGrpcStatusFromContext
        ),
      )

    def partiesNotKnownToLedger(parties: Set[String])(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      errorCodesVersionSwitcher.choose(
        v1 = RpcStatus
          .of(
            Code.INVALID_ARGUMENT.value(),
            s"Parties not known on ledger: ${parties.mkString("[", ", ", "]")}",
            Seq.empty,
          ),
        v2 = GrpcStatus.toProto(
          LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
            .Reject(parties)
            .asGrpcStatusFromContext
        ),
      )

    def submitterCannotActViaParticipant(reason: String)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      errorCodesVersionSwitcher.choose(
        v1 = RpcStatus.of(
          Code.PERMISSION_DENIED.value(),
          s"Submitted cannot act via participant: $reason",
          Seq.empty,
        ),
        v2 = GrpcStatus.toProto(
          LedgerApiErrors.WriteServiceRejections.SubmitterCannotActViaParticipant
            .Reject(reason)
            .asGrpcStatusFromContext
        ),
      )

    def invalidLedgerTime(reason: String)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      errorCodesVersionSwitcher.choose(
        v1 = RpcStatus.of(Code.ABORTED.value(), s"Invalid ledger time: $reason", Seq.empty),
        v2 = GrpcStatus.toProto(
          LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
            .RejectSimple(reason)
            .asGrpcStatusFromContext
        ),
      )

    def invalidLedgerTime(
        v1Status: RpcStatus,
        ledgerTime: Instant,
        ledgerTimeLowerBound: Instant,
        ledgerTimeUpperBound: Instant,
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status = {
      val details =
        s"Ledger time $ledgerTime outside of range [$ledgerTimeLowerBound, $ledgerTimeUpperBound]"
      errorCodesVersionSwitcher.choose(
        v1 = v1Status,
        v2 = GrpcStatus.toProto(
          LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
            .RejectEnriched(
              details,
              ledgerTime,
              ledgerTimeLowerBound,
              ledgerTimeUpperBound,
            )
            .asGrpcStatusFromContext
        ),
      )
    }

    def inconsistent(reason: String)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      errorCodesVersionSwitcher.choose(
        v1 = RpcStatus.of(Code.ABORTED.value(), s"Inconsistent: $reason", Seq.empty),
        v2 = GrpcStatus.toProto(
          LedgerApiErrors.ConsistencyErrors.Inconsistent.Reject(reason).asGrpcStatusFromContext
        ),
      )

    object Deprecated {
      @deprecated
      def disputed(reason: String)(implicit
          contextualizedErrorLogger: ContextualizedErrorLogger
      ): com.google.rpc.status.Status =
        errorCodesVersionSwitcher.choose(
          v1 = RpcStatus.of(Code.INVALID_ARGUMENT.value(), s"Disputed: $reason", Seq.empty),
          v2 = GrpcStatus.toProto(
            LedgerApiErrors.WriteServiceRejections.Disputed.Reject(reason).asGrpcStatusFromContext
          ),
        )

      @deprecated
      def outOfQuota(reason: String)(implicit
          contextualizedErrorLogger: ContextualizedErrorLogger
      ): com.google.rpc.status.Status =
        errorCodesVersionSwitcher.choose(
          v1 = RpcStatus.of(Code.ABORTED.value(), s"Resources exhausted: $reason", Seq.empty),
          v2 = GrpcStatus.toProto(
            LedgerApiErrors.WriteServiceRejections.OutOfQuota.Reject(reason).asGrpcStatusFromContext
          ),
        )
    }
  }
}

object ErrorFactories {
  val SelfServiceErrorCodeFactories: ErrorFactories = ErrorFactories(
    new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = true)
  )

  def apply(errorCodesVersionSwitcher: ErrorCodesVersionSwitcher): ErrorFactories =
    new ErrorFactories(errorCodesVersionSwitcher)

  def apply(useSelfServiceErrorCodes: Boolean): ErrorFactories =
    new ErrorFactories(
      new ErrorCodesVersionSwitcher(enableSelfServiceErrorCodes = useSelfServiceErrorCodes)
    )

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
