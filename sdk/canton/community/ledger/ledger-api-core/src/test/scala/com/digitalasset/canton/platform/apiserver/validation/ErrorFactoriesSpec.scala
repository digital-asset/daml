// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.validation

import com.daml.error.*
import com.daml.error.utils.ErrorDetails
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors.InvalidDeduplicationPeriodField.ValidMaxDeduplicationFieldKey
import com.digitalasset.canton.ledger.error.groups.{
  AdminServiceErrors,
  AuthorizationChecksErrors,
  ConsistencyErrors,
  RequestValidationErrors,
}
import com.digitalasset.canton.ledger.error.{CommonErrors, IndexErrors, LedgerApiErrors}
import com.digitalasset.canton.logging.{LedgerErrorLoggingContext, SuppressionRule}
import com.google.rpc.*
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level
import org.slf4j.event.Level.{ERROR, INFO}

import java.sql.{SQLNonTransientException, SQLTransientException}
import java.time.Duration
import scala.concurrent.duration.*

class ErrorFactoriesSpec
    extends AnyWordSpec
    with TableDrivenPropertyChecks
    with ErrorsAssertions
    with Eventually
    with IntegrationPatience
    with BaseTest {

  private val originalCorrelationId = "cor-id-12345679"
  private val truncatedCorrelationId = "cor-id-1"

  implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    LedgerErrorLoggingContext(
      logger,
      loggerFactory.properties,
      traceContext,
      originalCorrelationId,
    )

  private val expectedCorrelationIdRequestInfo =
    ErrorDetails.RequestInfoDetail(originalCorrelationId)
  private val expectedLocationRegex =
    """\{location=ErrorFactoriesSpec.scala:\d+\}"""
  private val expectedInternalErrorMessage =
    BaseError.SecuritySensitiveMessage(Some(originalCorrelationId))
  private val expectedInternalErrorDetails =
    Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo)

  "Errors " should {

    "return sqlTransientException" in {
      val failureReason = "some db transient failure"
      val someSqlTransientException = new SQLTransientException(failureReason)
      val msg =
        s"INDEX_DB_SQL_TRANSIENT_ERROR(1,$truncatedCorrelationId): Processing the request failed due to a transient database error: $failureReason"
      assertError(
        IndexErrors.DatabaseErrors.SqlTransientError
          .Reject(someSqlTransientException)(contextualizedErrorLogger)
      )(
        code = Code.UNAVAILABLE,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1.second),
          ErrorDetails.ErrorInfoDetail(
            "INDEX_DB_SQL_TRANSIENT_ERROR",
            Map(
              "category" -> "1",
              "definite_answer" -> "false",
              "test" -> getClass.getSimpleName,
            ),
          ),
        ),
        logLevel = INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return sqlNonTransientException" in {
      val failureReason = "some db non-transient failure"
      val msg =
        s"INDEX_DB_SQL_NON_TRANSIENT_ERROR(4,$truncatedCorrelationId): Processing the request failed due to a non-transient database error: $failureReason"
      assertError(
        IndexErrors.DatabaseErrors.SqlNonTransientError
          .Reject(
            new SQLNonTransientException(failureReason)
          )(contextualizedErrorLogger)
      )(
        code = Code.INTERNAL,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logLevel = ERROR,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "TrackerErrors" should {
      "return failedToEnqueueCommandSubmission" in {
        val t = new Exception("message123")
        assertStatus(
          LedgerApiErrors.InternalError
            .Generic("some message", Some(t))(
              contextualizedErrorLogger
            )
            .asGrpcStatus
        )(
          code = Code.INTERNAL,
          message = expectedInternalErrorMessage,
          details = expectedInternalErrorDetails,
          logLevel = Level.ERROR,
          logMessage = s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): some message",
          logErrorContextRegEx =
            expectedErrContextRegex("""throwableO=Some\(java.lang.Exception: message123\)"""),
        )
      }
    }

    "return bufferFul" in {
      val msg =
        s"PARTICIPANT_BACKPRESSURE(2,$truncatedCorrelationId): The participant is overloaded: Some buffer is full"
      assertStatus(
        LedgerApiErrors.ParticipantBackpressure
          .Rejection("Some buffer is full")(contextualizedErrorLogger)
          .asGrpcStatus
      )(
        code = Code.ABORTED,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "PARTICIPANT_BACKPRESSURE",
            Map(
              "category" -> "2",
              "definite_answer" -> "false",
              "reason" -> "Some buffer is full",
              "test" -> getClass.getSimpleName,
            ),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1.second),
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedErrContextRegex("reason=Some buffer is full"),
      )
    }

    "return queueClosed" in {
      val msg =
        s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): Some service is not running."
      assertStatus(
        CommonErrors.ServiceNotRunning
          .Reject("Some service")(
            contextualizedErrorLogger
          )
          .asGrpcStatus
      )(
        code = Code.UNAVAILABLE,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "SERVICE_NOT_RUNNING",
            Map(
              "category" -> "1",
              "definite_answer" -> "false",
              "service_name" -> "Some service",
              "test" -> getClass.getSimpleName,
            ),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1.second),
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedErrContextRegex("service_name=Some service"),
      )
    }

    "return timeout" in {
      val msg =
        s"REQUEST_TIME_OUT(3,$truncatedCorrelationId): Timed out while awaiting for a completion corresponding to a command submission."
      assertStatus(
        CommonErrors.RequestTimeOut
          .Reject(
            "Timed out while awaiting for a completion corresponding to a command submission.",
            definiteAnswer = false,
          )(
            contextualizedErrorLogger
          )
          .asGrpcStatus
      )(
        code = Code.DEADLINE_EXCEEDED,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "REQUEST_TIME_OUT",
            Map("category" -> "3", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1.second),
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return noStatusInResponse" in {
      assertStatus(
        LedgerApiErrors.InternalError
          .Generic(
            "Missing status in completion response.",
            throwableO = None,
          )(contextualizedErrorLogger)
          .asGrpcStatus
      )(
        code = Code.INTERNAL,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logLevel = Level.ERROR,
        logMessage =
          s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): Missing status in completion response.",
        logErrorContextRegEx = expectedErrContextRegex("throwableO=None"),
      )

    }

    "return packageNotFound" in {
      val msg = s"PACKAGE_NOT_FOUND(11,$truncatedCorrelationId): Could not find package."
      assertError(
        RequestValidationErrors.NotFound.Package
          .Reject("packageId123")(contextualizedErrorLogger)
      )(
        code = Code.NOT_FOUND,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "PACKAGE_NOT_FOUND",
            Map("category" -> "11", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.ResourceInfoDetail(typ = "PACKAGE", name = "packageId123"),
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return the a versioned service internal error" in {
      assertError(
        LedgerApiErrors.InternalError.VersionService("message123")(contextualizedErrorLogger)
      )(
        code = Code.INTERNAL,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logLevel = Level.ERROR,
        logMessage = s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): message123",
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return the configurationEntryRejected" in {
      val msg = s"CONFIGURATION_ENTRY_REJECTED(9,$truncatedCorrelationId): message123"
      assertError(
        AdminServiceErrors.ConfigurationEntryRejected.Reject("message123")(
          contextualizedErrorLogger
        )
      )(
        code = Code.FAILED_PRECONDITION,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "CONFIGURATION_ENTRY_REJECTED",
            Map("category" -> "9", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return a transactionNotFound error" in {
      val msg =
        s"TRANSACTION_NOT_FOUND(11,$truncatedCorrelationId): Transaction not found, or not visible."
      assertError(
        RequestValidationErrors.NotFound.Transaction
          .Reject(Ref.TransactionId.assertFromString("tId"))(contextualizedErrorLogger)
      )(
        code = Code.NOT_FOUND,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "TRANSACTION_NOT_FOUND",
            Map("category" -> "11", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.ResourceInfoDetail(typ = "TRANSACTION_ID", name = "tId"),
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return the DuplicateCommandException" in {
      val msg =
        s"DUPLICATE_COMMAND(10,$truncatedCorrelationId): A command with the given command id has already been successfully processed"
      assertError(
        ConsistencyErrors.DuplicateCommand
          .Reject(existingCommandSubmissionId = None)(contextualizedErrorLogger)
      )(
        code = Code.ALREADY_EXISTS,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "DUPLICATE_COMMAND",
            Map("category" -> "10", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return a permissionDenied error" in {
      assertError(
        AuthorizationChecksErrors.PermissionDenied.Reject("some cause")(
          contextualizedErrorLogger
        )
      )(
        code = Code.PERMISSION_DENIED,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logLevel = Level.WARN,
        logMessage = s"PERMISSION_DENIED(7,$truncatedCorrelationId): some cause",
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return a isTimeoutUnknown_wasAborted error" in {
      val msg = s"REQUEST_TIME_OUT(3,$truncatedCorrelationId): message123"
      assertError(
        CommonErrors.RequestTimeOut
          .Reject("message123", definiteAnswer = false)(contextualizedErrorLogger)
      )(
        code = Code.DEADLINE_EXCEEDED,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "REQUEST_TIME_OUT",
            Map("category" -> "3", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1.second),
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return a nonHexOffset error" in {
      val msg =
        s"NON_HEXADECIMAL_OFFSET(8,$truncatedCorrelationId): Offset in fieldName123 not specified in hexadecimal: offsetValue123: message123"
      assertError(
        RequestValidationErrors.NonHexOffset
          .Error(
            fieldName = "fieldName123",
            offsetValue = "offsetValue123",
            message = "message123",
          )(contextualizedErrorLogger)
          .asGrpcError
      )(
        code = Code.INVALID_ARGUMENT,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "NON_HEXADECIMAL_OFFSET",
            Map("category" -> "8", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return an offsetAfterLedgerEnd error" in {
      val expectedMessage = s"Absolute offset (AABBCC) is after ledger end (E)"
      val msg = s"OFFSET_AFTER_LEDGER_END(12,$truncatedCorrelationId): $expectedMessage"
      assertError(
        RequestValidationErrors.OffsetAfterLedgerEnd
          .Reject("Absolute", "AABBCC", "E")(contextualizedErrorLogger)
      )(
        code = Code.OUT_OF_RANGE,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "OFFSET_AFTER_LEDGER_END",
            Map("category" -> "12", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return a offsetOutOfRange error" in {
      val msg = s"OFFSET_OUT_OF_RANGE(9,$truncatedCorrelationId): message123"
      assertError(
        RequestValidationErrors.OffsetOutOfRange
          .Reject("message123")(contextualizedErrorLogger)
      )(
        code = Code.FAILED_PRECONDITION,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "OFFSET_OUT_OF_RANGE",
            Map("category" -> "9", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return an unauthenticatedMissingJwtToken error" in {
      assertError(
        AuthorizationChecksErrors.Unauthenticated
          .MissingJwtToken()(contextualizedErrorLogger)
      )(
        code = Code.UNAUTHENTICATED,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        Level.WARN,
        s"UNAUTHENTICATED(6,$truncatedCorrelationId): The command is missing a (valid) JWT token",
        expectedLocationRegex,
      )
    }

    "return an internalAuthenticationError" in {
      val someSecuritySafeMessage = "nothing security sensitive in here"
      val someThrowable = new RuntimeException("some internal authentication error")
      assertError(
        AuthorizationChecksErrors.InternalAuthorizationError
          .Reject(someSecuritySafeMessage, someThrowable)(contextualizedErrorLogger)
      )(
        code = Code.INTERNAL,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logLevel = Level.ERROR,
        logMessage =
          s"INTERNAL_AUTHORIZATION_ERROR(4,$truncatedCorrelationId): nothing security sensitive in here",
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return an invalid deduplication period error" in {
      val errorDetailMessage = "message"
      val maxDeduplicationDuration = Duration.ofSeconds(5)
      val msg =
        s"INVALID_DEDUPLICATION_PERIOD(9,$truncatedCorrelationId): The submitted command had an invalid deduplication period: $errorDetailMessage"
      assertError(
        RequestValidationErrors.InvalidDeduplicationPeriodField
          .Reject(
            reason = errorDetailMessage,
            maxDeduplicationDuration = Some(maxDeduplicationDuration),
          )(contextualizedErrorLogger)
          .asGrpcError
      )(
        code = Code.FAILED_PRECONDITION,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "INVALID_DEDUPLICATION_PERIOD",
            Map(
              "category" -> "9",
              "definite_answer" -> "false",
              ValidMaxDeduplicationFieldKey -> maxDeduplicationDuration.toString,
              "test" -> getClass.getSimpleName,
            ),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedErrContextRegex("longest_duration=PT5S"),
      )
    }

    "return an invalidField error" in {
      val fieldName = "my field"
      val msg =
        s"INVALID_FIELD(8,$truncatedCorrelationId): The submitted command has a field with invalid value: Invalid field $fieldName: my message"
      assertError(
        RequestValidationErrors.InvalidField
          .Reject(fieldName, "my message")(contextualizedErrorLogger)
      )(
        code = Code.INVALID_ARGUMENT,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "INVALID_FIELD",
            Map("category" -> "8", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

    "return a participantPrunedDataAccessed error" in {
      val msg = s"PARTICIPANT_PRUNED_DATA_ACCESSED(9,$truncatedCorrelationId): my message"
      assertError(
        RequestValidationErrors.ParticipantPrunedDataAccessed
          .Reject(
            "my message",
            "00",
          )(contextualizedErrorLogger)
      )(
        code = Code.FAILED_PRECONDITION,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "PARTICIPANT_PRUNED_DATA_ACCESSED",
            Map(
              "category" -> "9",
              "definite_answer" -> "false",
              LedgerApiErrors.EarliestOffsetMetadataKey -> "00",
              "test" -> getClass.getSimpleName,
            ),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx =
          expectedErrContextRegex(s"${LedgerApiErrors.EarliestOffsetMetadataKey}=00"),
      )
    }

    "return a trackerFailure error" in {
      assertError(LedgerApiErrors.InternalError.Generic("message123")(contextualizedErrorLogger))(
        code = Code.INTERNAL,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logLevel = Level.ERROR,
        logMessage = s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): message123",
        logErrorContextRegEx = expectedErrContextRegex("throwableO=None"),
      )
    }

    "return a serviceNotRunning error" in {
      val serviceName = "Some API Service"

      val msg =
        s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): $serviceName is not running."
      assertError(CommonErrors.ServiceNotRunning.Reject(serviceName)(contextualizedErrorLogger))(
        code = Code.UNAVAILABLE,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "SERVICE_NOT_RUNNING",
            Map(
              "category" -> "1",
              "definite_answer" -> "false",
              "service_name" -> serviceName,
              "test" -> getClass.getSimpleName,
            ),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1.second),
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedErrContextRegex("service_name=Some API Service"),
      )
    }

    "return a missingField error" in {
      val fieldName = "my field"

      val msg =
        s"MISSING_FIELD(8,$truncatedCorrelationId): The submitted command is missing a mandatory field: $fieldName"
      assertError(
        RequestValidationErrors.MissingField
          .Reject(fieldName)(contextualizedErrorLogger)
      )(
        code = Code.INVALID_ARGUMENT,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "MISSING_FIELD",
            Map(
              "category" -> "8",
              "definite_answer" -> "false",
              "field_name" -> fieldName,
              "test" -> getClass.getSimpleName,
            ),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedErrContextRegex("field_name=my field"),
      )
    }

    val msg =
      s"INVALID_ARGUMENT(8,$truncatedCorrelationId): The submitted request has invalid arguments: my message"
    "return an invalidArgument error" in {
      assertError(
        RequestValidationErrors.InvalidArgument
          .Reject("my message")(contextualizedErrorLogger)
      )(
        code = Code.INVALID_ARGUMENT,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "INVALID_ARGUMENT",
            Map("category" -> "8", "definite_answer" -> "false", "test" -> getClass.getSimpleName),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logLevel = Level.INFO,
        logMessage = msg,
        logErrorContextRegEx = expectedLocationRegex,
      )
    }

  }

  private def expectedErrContextRegex(extraInner: String): String = {
    val locationRegex = "location=ErrorFactoriesSpec.scala:\\d+"
    List(extraInner, locationRegex).sorted.mkString("""\{""", ", ", """\}""")
  }

  private def assertStatus(status: => Status)(
      code: Code,
      message: String,
      details: Seq[ErrorDetails.ErrorDetail],
      logLevel: Level,
      logMessage: String,
      logErrorContextRegEx: String,
  ): Unit = {
    lazy val e = io.grpc.protobuf.StatusProto.toStatusRuntimeException(status)
    assertError(new ErrorCode.ApiException(e.getStatus, e.getTrailers))(
      code,
      message,
      details,
      logLevel,
      logMessage,
      logErrorContextRegEx,
    )
  }

  private def assertError(
      error: => DamlError
  )(
      code: Code,
      message: String,
      details: Seq[ErrorDetails.ErrorDetail],
      logLevel: Level,
      logMessage: String,
      logErrorContextRegEx: String,
  ): Unit =
    loggerFactory.assertLogs(SuppressionRule.Level(logLevel))(
      within = assertError(
        actual = error.asGrpcError,
        expectedStatusCode = code,
        expectedMessage = message,
        expectedDetails = details,
      ),
      assertions = logEntry => {
        logEntry.level shouldBe logLevel
        logEntry.message shouldBe logMessage
        logEntry.mdc.keys should contain("err-context")
        logEntry.mdc
          .get("err-context")
          .value should fullyMatch regex logErrorContextRegEx
      },
    )

  private def assertError(
      statusRuntimeException: => StatusRuntimeException
  )(
      code: Code,
      message: String,
      details: Seq[ErrorDetails.ErrorDetail],
      logLevel: Level,
      logMessage: String,
      logErrorContextRegEx: String,
  )(implicit d: DummyImplicit): Unit =
    loggerFactory.assertLogs(SuppressionRule.Level(logLevel))(
      within = assertError(
        actual = statusRuntimeException,
        expectedStatusCode = code,
        expectedMessage = message,
        expectedDetails = details,
      ),
      assertions = logEntry => {
        logEntry.level shouldBe logLevel
        logEntry.message shouldBe logMessage
        logEntry.mdc.keys should contain("err-context")
        logEntry.mdc
          .get("err-context")
          .value should fullyMatch regex logErrorContextRegEx
      },
    )
}
