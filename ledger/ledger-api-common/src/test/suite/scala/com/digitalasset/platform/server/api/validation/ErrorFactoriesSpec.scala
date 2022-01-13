// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.sql.{SQLNonTransientException, SQLTransientException}
import java.time.Duration
import java.util.regex.Pattern

import ch.qos.logback.classic.Level
import com.daml.error.definitions.LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField.ValidMaxDeduplicationFieldKey
import com.daml.error.utils.ErrorDetails
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
  ErrorsAssertions,
}
import com.daml.ledger.api.domain.LedgerId
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories._
import com.daml.platform.testing.LogCollector.ExpectedLogEntry
import com.daml.platform.testing.{LogCollector, LogCollectorAssertions}
import com.google.rpc._
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

@nowarn("msg=deprecated")
class ErrorFactoriesSpec
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with MockitoSugar
    with BeforeAndAfter
    with LogCollectorAssertions
    with ErrorsAssertions {

  private val logger = ContextualizedLogger.get(getClass)
  private val loggingContext = LoggingContext.ForTesting

  private val originalCorrelationId = "cor-id-12345679"
  private val truncatedCorrelationId = "cor-id-1"

  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, Some(originalCorrelationId))

  private val expectedCorrelationIdRequestInfo =
    ErrorDetails.RequestInfoDetail(originalCorrelationId)
  private val expectedLocationLogMarkerRegex =
    "\\{err-context: \"\\{location=ErrorFactories.scala:\\d+\\}\"\\}"
  private val expectedInternalErrorMessage =
    s"An error occurred. Please contact the operator and inquire about the request $originalCorrelationId"
  private val expectedInternalErrorDetails =
    Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo)

  private val tested = ErrorFactories(mock[ErrorCodesVersionSwitcher])

  before {
    LogCollector.clear[this.type]
  }

  "ErrorFactories" should {

    "return sqlTransientException" in {
      val failureReason = "some db transient failure"
      val someSqlTransientException = new SQLTransientException(failureReason)
      val msg =
        s"INDEX_DB_SQL_TRANSIENT_ERROR(1,$truncatedCorrelationId): Processing the request failed due to a transient database error: $failureReason"
      assertV2Error(
        SelfServiceErrorCodeFactories.sqlTransientException(someSqlTransientException)
      )(
        expectedCode = Code.UNAVAILABLE,
        expectedMessage = msg,
        expectedDetails = Seq[ErrorDetails.ErrorDetail](
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1),
          ErrorDetails.ErrorInfoDetail(
            "INDEX_DB_SQL_TRANSIENT_ERROR",
            Map("category" -> "1", "definite_answer" -> "false"),
          ),
        ),
        expectedLogEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return sqlNonTransientException" in {
      val failureReason = "some db non-transient failure"
      val msg =
        s"INDEX_DB_SQL_NON_TRANSIENT_ERROR(4,$truncatedCorrelationId): Processing the request failed due to a non-transient database error: $failureReason"
      assertV2Error(
        SelfServiceErrorCodeFactories
          .sqlNonTransientException(new SQLNonTransientException(failureReason))
      )(
        expectedCode = Code.INTERNAL,
        expectedMessage = expectedInternalErrorMessage,
        expectedDetails = expectedInternalErrorDetails,
        expectedLogEntry = ExpectedLogEntry(
          Level.ERROR,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "TrackerErrors" should {
      val errorDetails = com.google.protobuf.Any.pack[ErrorInfo](ErrorInfo.newBuilder().build())

      "return failedToEnqueueCommandSubmission" in {
        val t = new Exception("message123")
        assertVersionedStatus(
          _.SubmissionQueueErrors.failedToEnqueueCommandSubmission("some message")(t)(
            contextualizedErrorLogger
          )
        )(
          v1_code = Code.ABORTED,
          v1_message = "some message: Exception: message123",
          v1_details = Seq(errorDetails),
          v2_code = Code.INTERNAL,
          v2_message = expectedInternalErrorMessage,
          v2_details = expectedInternalErrorDetails,
          v2_logEntry = ExpectedLogEntry(
            Level.ERROR,
            s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): some message: Exception: message123",
            expectedMarkerRegex("throwableO=Some(java.lang.Exception: message123)"),
          ),
        )
      }

      "return bufferFul" in {
        val msg =
          s"PARTICIPANT_BACKPRESSURE(2,$truncatedCorrelationId): The participant is overloaded: Some buffer is full"
        assertVersionedStatus(
          _.bufferFull("Some buffer is full")(contextualizedErrorLogger)
        )(
          v1_code = Code.RESOURCE_EXHAUSTED,
          v1_message = "Ingress buffer is full",
          v1_details = Seq(errorDetails),
          v2_code = Code.ABORTED,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "PARTICIPANT_BACKPRESSURE",
              Map(
                "category" -> "2",
                "definite_answer" -> "false",
                "reason" -> "Some buffer is full",
              ),
            ),
            expectedCorrelationIdRequestInfo,
            ErrorDetails.RetryInfoDetail(1),
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.WARN,
            msg,
            expectedMarkerRegex("reason=Some buffer is full"),
          ),
        )
      }

      "return queueClosed" in {
        val msg =
          s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): Some service has been shut down."
        assertVersionedStatus(
          _.SubmissionQueueErrors.queueClosed("Some service")(
            contextualizedErrorLogger = contextualizedErrorLogger
          )
        )(
          v1_code = Code.ABORTED,
          v1_message = "Queue closed",
          v1_details = Seq(errorDetails),
          v2_code = Code.UNAVAILABLE,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "SERVICE_NOT_RUNNING",
              Map(
                "category" -> "1",
                "definite_answer" -> "false",
                "service_name" -> "Some service",
              ),
            ),
            expectedCorrelationIdRequestInfo,
            ErrorDetails.RetryInfoDetail(1),
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            expectedMarkerRegex("service_name=Some service"),
          ),
        )
      }

      "return timeout" in {
        val msg =
          s"REQUEST_TIME_OUT(3,$truncatedCorrelationId): Timed out while awaiting for a completion corresponding to a command submission."
        assertVersionedStatus(
          _.SubmissionQueueErrors.timedOutOnAwaitingForCommandCompletion()(
            contextualizedErrorLogger = contextualizedErrorLogger
          )
        )(
          v1_code = Code.ABORTED,
          v1_message = "Timeout",
          v1_details = Seq(errorDetails),
          v2_code = Code.DEADLINE_EXCEEDED,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "REQUEST_TIME_OUT",
              Map("category" -> "3", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
            ErrorDetails.RetryInfoDetail(1),
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            Some(expectedLocationLogMarkerRegex),
          ),
        )
      }
      "return noStatusInResponse" in {
        assertVersionedStatus(
          _.SubmissionQueueErrors.noStatusInCompletionResponse()(
            contextualizedErrorLogger = contextualizedErrorLogger
          )
        )(
          v1_code = Code.INTERNAL,
          v1_message = "Missing status in completion response.",
          v1_details = Seq(),
          v2_code = Code.INTERNAL,
          v2_message = expectedInternalErrorMessage,
          v2_details = expectedInternalErrorDetails,
          v2_logEntry = ExpectedLogEntry(
            Level.ERROR,
            s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): Missing status in completion response.",
            expectedMarkerRegex("throwableO=None"),
          ),
        )

      }

    }

    "return packageNotFound" in {
      val msg = s"PACKAGE_NOT_FOUND(11,$truncatedCorrelationId): Could not find package."
      assertVersionedError(_.packageNotFound("packageId123"))(
        v1_code = Code.NOT_FOUND,
        v1_message = "",
        v1_details = Seq.empty,
        v2_code = Code.NOT_FOUND,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "PACKAGE_NOT_FOUND",
            Map("category" -> "11", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.ResourceInfoDetail("PACKAGE", "packageId123"),
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return the a versioned service internal error" in {
      assertVersionedError(_.versionServiceInternalError("message123"))(
        v1_code = Code.INTERNAL,
        v1_message = "message123",
        v1_details = Seq.empty,
        v2_code = Code.INTERNAL,
        v2_message = expectedInternalErrorMessage,
        v2_details = expectedInternalErrorDetails,
        v2_logEntry = ExpectedLogEntry(
          Level.ERROR,
          s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): message123",
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return the configurationEntryRejected" in {
      val msg = s"CONFIGURATION_ENTRY_REJECTED(9,$truncatedCorrelationId): message123"
      assertVersionedError(_.configurationEntryRejected("message123", None))(
        v1_code = Code.ABORTED,
        v1_message = "message123",
        v1_details = Seq.empty,
        v2_code = Code.FAILED_PRECONDITION,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "CONFIGURATION_ENTRY_REJECTED",
            Map("category" -> "9", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a transactionNotFound error" in {
      val msg =
        s"TRANSACTION_NOT_FOUND(11,$truncatedCorrelationId): Transaction not found, or not visible."
      assertVersionedError(_.transactionNotFound(Ref.TransactionId.assertFromString("tId")))(
        v1_code = Code.NOT_FOUND,
        v1_message = "Transaction not found, or not visible.",
        v1_details = Seq.empty,
        v2_code = Code.NOT_FOUND,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "TRANSACTION_NOT_FOUND",
            Map("category" -> "11", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.ResourceInfoDetail("TRANSACTION_ID", "tId"),
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return the DuplicateCommandException" in {
      val msg =
        s"DUPLICATE_COMMAND(10,$truncatedCorrelationId): A command with the given command id has already been successfully processed"
      assertVersionedError(_.duplicateCommandException(None))(
        v1_code = Code.ALREADY_EXISTS,
        v1_message = "Duplicate command",
        v1_details = Seq(definiteAnswers(false)),
        v2_code = Code.ALREADY_EXISTS,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "DUPLICATE_COMMAND",
            Map("category" -> "10", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a permissionDenied error" in {
      assertVersionedError(_.permissionDenied("some cause"))(
        v1_code = Code.PERMISSION_DENIED,
        v1_message = "",
        v1_details = Seq.empty,
        v2_code = Code.PERMISSION_DENIED,
        v2_message = expectedInternalErrorMessage,
        v2_details = expectedInternalErrorDetails,
        v2_logEntry = ExpectedLogEntry(
          Level.WARN,
          s"PERMISSION_DENIED(7,$truncatedCorrelationId): some cause",
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a isTimeoutUnknown_wasAborted error" in {
      val msg = s"REQUEST_TIME_OUT(3,$truncatedCorrelationId): message123"
      assertVersionedError(
        _.isTimeoutUnknown_wasAborted("message123", definiteAnswer = Some(false))
      )(
        v1_code = Code.ABORTED,
        v1_message = "message123",
        v1_details = Seq(definiteAnswers(false)),
        v2_code = Code.DEADLINE_EXCEEDED,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "REQUEST_TIME_OUT",
            Map("category" -> "3", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1),
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a nonHexOffset error" in {
      val msg =
        s"NON_HEXADECIMAL_OFFSET(8,$truncatedCorrelationId): Offset in fieldName123 not specified in hexadecimal: offsetValue123: message123"
      assertVersionedError(
        _.nonHexOffset(None)(
          fieldName = "fieldName123",
          offsetValue = "offsetValue123",
          message = "message123",
        )
      )(
        v1_code = Code.INVALID_ARGUMENT,
        v1_message = "Invalid argument: message123",
        v1_details = Seq.empty,
        v2_code = Code.INVALID_ARGUMENT,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail("NON_HEXADECIMAL_OFFSET", Map("category" -> "8")),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return an offsetAfterLedgerEnd error" in {
      val expectedMessage = s"Absolute offset (AABBCC) is after ledger end (E)"
      val msg = s"OFFSET_AFTER_LEDGER_END(12,$truncatedCorrelationId): $expectedMessage"
      assertVersionedError(_.offsetAfterLedgerEnd("Absolute", "AABBCC", "E"))(
        v1_code = Code.OUT_OF_RANGE,
        v1_message = expectedMessage,
        v1_details = Seq.empty,
        v2_code = Code.OUT_OF_RANGE,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "OFFSET_AFTER_LEDGER_END",
            Map("category" -> "12", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a offsetOutOfRange error" in {
      val msg = s"OFFSET_OUT_OF_RANGE(9,$truncatedCorrelationId): message123"
      assertVersionedError(_.offsetOutOfRange(None)("message123"))(
        v1_code = Code.INVALID_ARGUMENT,
        v1_message = "Invalid argument: message123",
        v1_details = Seq.empty,
        v2_code = Code.FAILED_PRECONDITION,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "OFFSET_OUT_OF_RANGE",
            Map("category" -> "9", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return an unauthenticatedMissingJwtToken error" in {
      assertVersionedError(_.unauthenticatedMissingJwtToken())(
        v1_code = Code.UNAUTHENTICATED,
        v1_message = "",
        v1_details = Seq.empty,
        v2_code = Code.UNAUTHENTICATED,
        v2_message = expectedInternalErrorMessage,
        v2_details = expectedInternalErrorDetails,
        v2_logEntry = ExpectedLogEntry(
          Level.WARN,
          s"UNAUTHENTICATED(6,$truncatedCorrelationId): The command is missing a (valid) JWT token",
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return an internalAuthenticationError" in {
      val someSecuritySafeMessage = "nothing security sensitive in here"
      val someThrowable = new RuntimeException("some internal authentication error")
      assertVersionedError(_.internalAuthenticationError(someSecuritySafeMessage, someThrowable))(
        v1_code = Code.INTERNAL,
        v1_message = someSecuritySafeMessage,
        v1_details = Seq.empty,
        v2_code = Code.INTERNAL,
        v2_message = expectedInternalErrorMessage,
        v2_details = expectedInternalErrorDetails,
        v2_logEntry = ExpectedLogEntry(
          Level.ERROR,
          s"INTERNAL_AUTHORIZATION_ERROR(4,$truncatedCorrelationId): nothing security sensitive in here",
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a missingLedgerConfig error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      val legacyErrorCode = Code.UNAVAILABLE

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val msg =
          s"LEDGER_CONFIGURATION_NOT_FOUND(11,$truncatedCorrelationId): The ledger configuration could not be retrieved."
        assertVersionedError(_.missingLedgerConfig(legacyErrorCode)(definiteAnswer))(
          v1_code = legacyErrorCode,
          v1_message = "The ledger configuration is not available.",
          v1_details = expectedDetails,
          v2_code = Code.NOT_FOUND,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "LEDGER_CONFIGURATION_NOT_FOUND",
              Map("category" -> "11", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            Some(expectedLocationLogMarkerRegex),
          ),
        )
      }
    }

    "return an aborted error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val exception = tested.aborted("my message", definiteAnswer)
        val status = StatusProto.fromThrowable(exception)
        status.getCode shouldBe Code.ABORTED.value()
        status.getMessage shouldBe "my message"
        status.getDetailsList.asScala shouldBe expectedDetails
      }
    }

    "return an invalid deduplication period error" in {
      val errorDetailMessage = "message"
      val field = "field"
      val maxDeduplicationDuration = Duration.ofSeconds(5)
      val msg =
        s"INVALID_DEDUPLICATION_PERIOD(9,$truncatedCorrelationId): The submitted command had an invalid deduplication period: $errorDetailMessage"
      assertVersionedError(
        _.invalidDeduplicationDuration(
          fieldName = field,
          message = errorDetailMessage,
          definiteAnswer = None,
          maxDeduplicationDuration = Some(maxDeduplicationDuration),
        )
      )(
        v1_code = Code.INVALID_ARGUMENT,
        v1_message = s"Invalid field $field: $errorDetailMessage",
        v1_details = Seq.empty,
        v2_code = Code.FAILED_PRECONDITION,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "INVALID_DEDUPLICATION_PERIOD",
            Map(
              "category" -> "9",
              "definite_answer" -> "false",
              ValidMaxDeduplicationFieldKey -> maxDeduplicationDuration.toString,
            ),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          expectedMarkerRegex("longest_duration=PT5S"),
        ),
      )
    }

    "return an invalidField error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      val fieldName = "my field"
      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val msg =
          s"INVALID_FIELD(8,$truncatedCorrelationId): The submitted command has a field with invalid value: Invalid field $fieldName: my message"
        assertVersionedError(_.invalidField(fieldName, "my message", definiteAnswer))(
          v1_code = Code.INVALID_ARGUMENT,
          v1_message = "Invalid field " + fieldName + ": my message",
          v1_details = expectedDetails,
          v2_code = Code.INVALID_ARGUMENT,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "INVALID_FIELD",
              Map("category" -> "8", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            Some(expectedLocationLogMarkerRegex),
          ),
        )
      }
    }

    "return a ledgerIdMismatch error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val msg =
          s"LEDGER_ID_MISMATCH(11,$truncatedCorrelationId): Ledger ID 'received' not found. Actual Ledger ID is 'expected'."
        assertVersionedError(
          _.ledgerIdMismatch(LedgerId("expected"), LedgerId("received"), definiteAnswer)
        )(
          v1_code = Code.NOT_FOUND,
          v1_message = "Ledger ID 'received' not found. Actual Ledger ID is 'expected'.",
          v1_details = expectedDetails,
          v2_code = Code.NOT_FOUND,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "LEDGER_ID_MISMATCH",
              Map("category" -> "11", "definite_answer" -> "true"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            Some(expectedLocationLogMarkerRegex),
          ),
        )
      }
    }

    "fail on creating a ledgerIdMismatch error due to a wrong definite answer" in {
      an[IllegalArgumentException] should be thrownBy tested.ledgerIdMismatch(
        LedgerId("expected"),
        LedgerId("received"),
        definiteAnswer = Some(true),
      )
    }

    "return a participantPrunedDataAccessed error" in {
      val msg = s"PARTICIPANT_PRUNED_DATA_ACCESSED(9,$truncatedCorrelationId): my message"
      assertVersionedError(_.participantPrunedDataAccessed("my message"))(
        v1_code = Code.NOT_FOUND,
        v1_message = "my message",
        v1_details = Seq.empty,
        v2_code = Code.FAILED_PRECONDITION,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "PARTICIPANT_PRUNED_DATA_ACCESSED",
            Map("category" -> "9", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a trackerFailure error" in {
      assertVersionedError(_.trackerFailure("message123"))(
        v1_code = Code.INTERNAL,
        v1_message = "message123",
        v1_details = Seq.empty,
        v2_code = Code.INTERNAL,
        v2_message = expectedInternalErrorMessage,
        v2_details = expectedInternalErrorDetails,
        v2_logEntry = ExpectedLogEntry(
          Level.ERROR,
          s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): message123",
          expectedMarkerRegex("throwableO=None"),
        ),
      )
    }

    "return a serviceNotRunning error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )
      val serviceName = "Some API Service"

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val msg =
          s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): $serviceName has been shut down."
        assertVersionedError(_.serviceNotRunning(serviceName)(definiteAnswer))(
          v1_code = Code.UNAVAILABLE,
          v1_message = s"$serviceName has been shut down.",
          v1_details = expectedDetails,
          v2_code = Code.UNAVAILABLE,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "SERVICE_NOT_RUNNING",
              Map("category" -> "1", "definite_answer" -> "false", "service_name" -> serviceName),
            ),
            expectedCorrelationIdRequestInfo,
            ErrorDetails.RetryInfoDetail(1),
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            expectedMarkerRegex("service_name=Some API Service"),
          ),
        )
      }
    }

    "return a serviceIsBeingReset error" in {
      val serviceName = "Some API Service"
      val someLegacyStatusCode = Code.CANCELLED

      val msg =
        s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): $serviceName is currently being reset."
      assertVersionedError(_.serviceIsBeingReset(someLegacyStatusCode.value())(serviceName))(
        v1_code = someLegacyStatusCode,
        v1_message = s"$serviceName is currently being reset.",
        v1_details = Seq.empty,
        v2_code = Code.UNAVAILABLE,
        v2_message = msg,
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "SERVICE_NOT_RUNNING",
            Map("category" -> "1", "definite_answer" -> "false", "service_name" -> serviceName),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1),
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          expectedMarkerRegex("service_name=Some API Service"),
        ),
      )
    }

    "return a missingField error" in {
      val fieldName = "my field"

      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        val msg =
          s"MISSING_FIELD(8,$truncatedCorrelationId): The submitted command is missing a mandatory field: $fieldName"
        assertVersionedError(_.missingField(fieldName, definiteAnswer))(
          v1_code = Code.INVALID_ARGUMENT,
          v1_message = "Missing field: " + fieldName,
          v1_details = expectedDetails,
          v2_code = Code.INVALID_ARGUMENT,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "MISSING_FIELD",
              Map("category" -> "8", "definite_answer" -> "false", "field_name" -> fieldName),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            expectedMarkerRegex("field_name=my field"),
          ),
        )
      }
    }

    val msg =
      s"INVALID_ARGUMENT(8,$truncatedCorrelationId): The submitted command has invalid arguments: my message"
    "return an invalidArgument error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        assertVersionedError(_.invalidArgument(definiteAnswer)("my message"))(
          v1_code = Code.INVALID_ARGUMENT,
          v1_message = "Invalid argument: my message",
          v1_details = expectedDetails,
          v2_code = Code.INVALID_ARGUMENT,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "INVALID_ARGUMENT",
              Map("category" -> "8", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            Some(expectedLocationLogMarkerRegex),
          ),
        )
      }
    }

    "return an invalidArgument (with legacy error code as NOT_FOUND) error" in {
      val testCases = Table(
        ("definite answer", "expected details"),
        (None, Seq.empty),
        (Some(false), Seq(definiteAnswers(false))),
      )

      forEvery(testCases) { (definiteAnswer, expectedDetails) =>
        assertVersionedError(_.invalidArgumentWasNotFound(definiteAnswer)("my message"))(
          v1_code = Code.NOT_FOUND,
          v1_message = "my message",
          v1_details = expectedDetails,
          v2_code = Code.INVALID_ARGUMENT,
          v2_message = msg,
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "INVALID_ARGUMENT",
              Map("category" -> "8", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            Some(expectedLocationLogMarkerRegex),
          ),
        )
      }
    }

    "should create an ApiException without the stack trace" in {
      val status = Status.newBuilder().setCode(Code.INTERNAL.value()).build()
      val exception = tested.grpcError(status)
      exception.getStackTrace shouldBe Array.empty
    }
  }

  private def expectedMarkerRegex(extraInner: String): Some[String] = {
    val locationRegex = "location=ErrorFactories.scala:\\d+"
    val inner = List(extraInner -> Pattern.quote(extraInner), locationRegex -> locationRegex)
      .sortBy(_._1)
      .map(_._2)
      .mkString("\"\\{", ", ", "\\}\"")
    Some(s"\\{err-context: $inner\\}")
  }

  private def assertVersionedError(
      error: ErrorFactories => StatusRuntimeException
  )(
      v1_code: Code,
      v1_message: String,
      v1_details: Seq[Any],
      v2_code: Code,
      v2_message: String,
      v2_details: Seq[ErrorDetails.ErrorDetail],
      v2_logEntry: ExpectedLogEntry,
  ): Unit = {
    val errorFactoriesV1 = ErrorFactories(new ErrorCodesVersionSwitcher(false))
    val errorFactoriesV2 = ErrorFactories(new ErrorCodesVersionSwitcher(true))
    assertV1Error(error(errorFactoriesV1))(v1_code, v1_message, v1_details)
    // Making sure we collect log entries for V2 only
    LogCollector.clear[this.type]
    assertV2Error(error(errorFactoriesV2))(v2_code, v2_message, v2_details, v2_logEntry)
  }

  private def assertVersionedStatus(
      error: ErrorFactories => Status
  )(
      v1_code: Code,
      v1_message: String,
      v1_details: Seq[Any],
      v2_code: Code,
      v2_message: String,
      v2_details: Seq[ErrorDetails.ErrorDetail],
      v2_logEntry: ExpectedLogEntry,
  ): Unit = {
    assertVersionedError(x => io.grpc.protobuf.StatusProto.toStatusRuntimeException(error(x)))(
      v1_code,
      v1_message,
      v1_details,
      v2_code,
      v2_message,
      v2_details,
      v2_logEntry,
    )

  }

  private def assertV1Error(
      statusRuntimeException: StatusRuntimeException
  )(expectedCode: Code, expectedMessage: String, expectedDetails: Seq[Any]): Unit = {
    val status = StatusProto.fromThrowable(statusRuntimeException)
    status.getCode shouldBe expectedCode.value()
    status.getMessage shouldBe expectedMessage
    val _ = status.getDetailsList.asScala shouldBe expectedDetails
  }

  private def assertV2Error(
      statusRuntimeException: StatusRuntimeException
  )(
      expectedCode: Code,
      expectedMessage: String,
      expectedDetails: Seq[ErrorDetails.ErrorDetail],
      expectedLogEntry: ExpectedLogEntry,
  ): Unit = {
    assertError[this.type, this.type](
      actual = statusRuntimeException,
      expectedCode,
      expectedMessage,
      expectedDetails,
      expectedLogEntry,
    )
  }
}
