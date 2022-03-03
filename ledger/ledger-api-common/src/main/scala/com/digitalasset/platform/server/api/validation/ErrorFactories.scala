// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.sql.{SQLNonTransientException, SQLTransientException}
import java.time.{Duration, Instant}

import com.daml.error.definitions.{IndexErrors, LedgerApiErrors}
import com.daml.error.ContextualizedErrorLogger
import com.daml.grpc.GrpcStatus
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.grpc.GrpcStatuses
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref.TransactionId
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.platform.server.api.{ApiException => NoStackTraceApiException}
import com.google.protobuf.{Any => AnyProto}
import com.google.rpc.{ErrorInfo, Status}
import io.grpc.Status.Code
import io.grpc.protobuf.StatusProto
import io.grpc.StatusRuntimeException
import scalaz.syntax.tag._

object ErrorFactories {

  object SubmissionQueueErrors {
    def failedToEnqueueCommandSubmission(message: String)(t: Throwable)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Status =
      LedgerApiErrors.InternalError
        .Generic(
          message = s"$message: ${t.getClass.getSimpleName}: ${t.getMessage}",
          throwableO = Some(t),
        )
        .asGrpcStatusFromContext

    def queueClosed(queueName: String)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Status =
      LedgerApiErrors.ServiceNotRunning
        .Reject(queueName)
        .asGrpcStatusFromContext

    def timedOutOnAwaitingForCommandCompletion()(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Status =
      LedgerApiErrors.RequestTimeOut
        .Reject(
          "Timed out while awaiting for a completion corresponding to a command submission.",
          _definiteAnswer = false,
        )
        .asGrpcStatusFromContext

    def noStatusInCompletionResponse()(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Status =
      LedgerApiErrors.InternalError
        .Generic(
          "Missing status in completion response.",
          throwableO = None,
        )
        .asGrpcStatusFromContext
  }

  def bufferFull(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Status =
    LedgerApiErrors.ParticipantBackpressure
      .Rejection(message)
      .asGrpcStatusFromContext

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
    LedgerApiErrors.RequestValidation.NotFound.Transaction
      .Reject(transactionId)
      .asGrpcError

  def packageNotFound(packageId: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.NotFound.Package
      .Reject(_packageId = packageId)
      .asGrpcError

  def versionServiceInternalError(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.InternalError.VersionService(message).asGrpcError

  def duplicateCommandException(existingSubmissionId: Option[String])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.ConsistencyErrors.DuplicateCommand
      .Reject(_existingCommandSubmissionId = existingSubmissionId)
      .asGrpcError

  /** @param expected Expected ledger id.
    * @param received  Received ledger id.
    * @return An exception with the [[Code.NOT_FOUND]] status code.
    */
  def ledgerIdMismatch(
      expected: LedgerId,
      received: LedgerId,
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusRuntimeException = {
    LedgerApiErrors.RequestValidation.LedgerIdMismatch
      .Reject(
        s"Ledger ID '${received.unwrap}' not found. Actual Ledger ID is '${expected.unwrap}'."
      )
      .asGrpcError
  }

  /** @param fieldName A missing field's name.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def missingField(fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.MissingField
      .Reject(fieldName)
      .asGrpcError

  /** @param message A status' message.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def invalidArgument(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.InvalidArgument
      .Reject(message)
      .asGrpcError

  // This error builder covers cases where existing logic handling invalid arguments returned NOT_FOUND.
  def invalidArgumentWasNotFound(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.InvalidArgument
      .Reject(message)
      .asGrpcError

  def offsetOutOfRange(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.OffsetOutOfRange
      .Reject(message)
      .asGrpcError

  def offsetAfterLedgerEnd(offsetType: String, requestedOffset: String, ledgerEnd: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.OffsetAfterLedgerEnd
      .Reject(offsetType, requestedOffset, ledgerEnd)
      .asGrpcError

  def nonHexOffset(fieldName: String, offsetValue: String, message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.NonHexOffset
      .Error(
        _fieldName = fieldName,
        _offsetValue = offsetValue,
        _message = message,
      )
      .asGrpcError

  def invalidDeduplicationPeriod(
      message: String,
      maxDeduplicationDuration: Option[Duration],
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField
      .Reject(message, maxDeduplicationDuration)
      .asGrpcError

  /** @param fieldName An invalid field's name.
    * @param message    A status' message.
    * @return An exception with the [[Code.INVALID_ARGUMENT]] status code.
    */
  def invalidField(
      fieldName: String,
      message: String,
  )(implicit contextualizedErrorLogger: ContextualizedErrorLogger): StatusRuntimeException =
    ledgerRequestValidationInvalidField(fieldName, message).asGrpcError

  private def ledgerRequestValidationInvalidField(fieldName: String, message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): LedgerApiErrors.RequestValidation.InvalidField.Reject = {
    LedgerApiErrors.RequestValidation.InvalidField
      .Reject(s"Invalid field $fieldName: $message")
  }

  /** @param message       A status' message.
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

  def isTimeoutUnknown_wasAborted(message: String, definiteAnswer: Option[Boolean])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestTimeOut
      .Reject(message, definiteAnswer.getOrElse(false))
      .asGrpcError

  def packageUploadRejected(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.AdminServices.PackageUploadRejected.Reject(message).asGrpcError

  def configurationEntryRejected(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.AdminServices.ConfigurationEntryRejected.Reject(message).asGrpcError

  // permission denied is intentionally without description to ensure we don't leak security relevant information by accident
  def permissionDenied(cause: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.AuthorizationChecks.PermissionDenied.Reject(cause).asGrpcError

  def unauthenticatedMissingJwtToken()(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.AuthorizationChecks.Unauthenticated
      .MissingJwtToken()
      .asGrpcError

  def internalAuthenticationError(securitySafeMessage: String, exception: Throwable)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.AuthorizationChecks.InternalAuthorizationError
      .Reject(securitySafeMessage, exception)
      .asGrpcError

  def missingLedgerConfig()(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.NotFound.LedgerConfiguration
      .Reject()
      .asGrpcError

  def missingLedgerConfig(message: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): com.google.rpc.status.Status =
    GrpcStatus.toProto(
      LedgerApiErrors.RequestValidation.NotFound.LedgerConfiguration
        .RejectWithMessage(message)
        .asGrpcStatusFromContext
    )

  def participantPrunedDataAccessed(message: String, earliestOffset: Offset)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.RequestValidation.ParticipantPrunedDataAccessed
      .Reject(message, earliestOffset.toHexString)
      .asGrpcError

  /** @return An exception with the [[Code.UNAVAILABLE]] status code.
    */
  def serviceNotRunning(serviceName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.ServiceNotRunning.Reject(serviceName).asGrpcError

  def trackerFailure(msg: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): StatusRuntimeException =
    LedgerApiErrors.InternalError.Generic(msg).asGrpcError

  /** Transforms Protobuf [[Status]] objects, possibly including metadata packed as [[ErrorInfo]] objects,
    * into exceptions with metadata in the trailers.
    *
    * Asynchronous errors, i.e. failed completions, contain Protobuf [[Status]] objects themselves.
    *
    * NOTE: The length of the Status message is truncated to a reasonable size for satisfying
    * the Netty header size limit - as the message is also incorporated in the header, bundled in the gRPC metadata.
    *
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
      GrpcStatus.toProto(
        LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
          .RejectDeprecated(reason)
          .asGrpcStatusFromContext
      )

    def contractsNotFound(missingContractIds: Set[String])(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.ContractNotFound
          .MultipleContractsNotFound(missingContractIds)
          .asGrpcStatusFromContext
      )

    def inconsistentContractKeys(
        lookupResult: Option[Value.ContractId],
        currentResult: Option[Value.ContractId],
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.InconsistentContractKey
          .Reject(
            s"Contract key lookup with different results: expected [$lookupResult], actual [$currentResult]"
          )
          .asGrpcStatusFromContext
      )

    def duplicateContractKey(reason: String, key: GlobalKey)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.DuplicateContractKey
          .RejectWithContractKeyArg(reason, key)
          .asGrpcStatusFromContext
      )

    def partiesNotKnownToLedger(parties: Set[String])(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      GrpcStatus.toProto(
        LedgerApiErrors.WriteServiceRejections.PartyNotKnownOnLedger
          .Reject(parties)
          .asGrpcStatusFromContext
      )

    def submitterCannotActViaParticipant(reason: String)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      GrpcStatus.toProto(
        LedgerApiErrors.WriteServiceRejections.SubmitterCannotActViaParticipant
          .Reject(reason)
          .asGrpcStatusFromContext
      )

    def invalidLedgerTime(reason: String)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
          .RejectSimple(reason)
          .asGrpcStatusFromContext
      )

    def invalidLedgerTime(
        ledgerTime: Instant,
        ledgerTimeLowerBound: Instant,
        ledgerTimeUpperBound: Instant,
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.InvalidLedgerTime
          .RejectEnriched(
            s"Ledger time $ledgerTime outside of range [$ledgerTimeLowerBound, $ledgerTimeUpperBound]",
            ledgerTime,
            ledgerTimeLowerBound,
            ledgerTimeUpperBound,
          )
          .asGrpcStatusFromContext
      )

    def inconsistent(reason: String)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): com.google.rpc.status.Status =
      GrpcStatus.toProto(
        LedgerApiErrors.ConsistencyErrors.Inconsistent.Reject(reason).asGrpcStatusFromContext
      )

    object Deprecated {
      @deprecated
      def disputed(reason: String)(implicit
          contextualizedErrorLogger: ContextualizedErrorLogger
      ): com.google.rpc.status.Status =
        GrpcStatus.toProto(
          LedgerApiErrors.WriteServiceRejections.Disputed.Reject(reason).asGrpcStatusFromContext
        )

      @deprecated
      def outOfQuota(reason: String)(implicit
          contextualizedErrorLogger: ContextualizedErrorLogger
      ): com.google.rpc.status.Status =
        GrpcStatus.toProto(
          LedgerApiErrors.WriteServiceRejections.OutOfQuota.Reject(reason).asGrpcStatusFromContext
        )
    }

  }

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
