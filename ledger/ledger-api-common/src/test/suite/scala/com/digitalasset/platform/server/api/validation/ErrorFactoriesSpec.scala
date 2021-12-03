// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.sql.{SQLNonTransientException, SQLTransientException}
import java.time.Duration

import ch.qos.logback.classic.Level
import com.daml.platform.testing.LogCollector.ExpectedLogEntry
import com.daml.platform.testing.LogCollectorAssertions
import com.daml.error.definitions.LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField.ValidMaxDeduplicationFieldKey
import com.daml.error.utils.ErrorDetails
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorCodesVersionSwitcher,
}
import com.daml.ledger.api.domain.LedgerId
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.server.api.validation.ErrorFactories._
import com.daml.platform.testing.LogCollector
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
    with LogCollectorAssertions {

  private val logger = ContextualizedLogger.get(getClass)
  private val loggingContext = LoggingContext.ForTesting

  private val originalCorrelationId = "cor-id-12345679"
  private val truncatedCorrelationId = "cor-id-1"

  private implicit val contextualizedErrorLogger: ContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, Some(originalCorrelationId))

  private val expectedCorrelationIdRequestInfo: ErrorDetails.RequestInfoDetail =
    ErrorDetails.RequestInfoDetail(originalCorrelationId)
  private val excpectedLocationLogMarkerRegex =
    "\\{err-context: \"\\{location=ErrorFactories.scala:\\d+\\}\"\\}"

  private val tested = ErrorFactories(mock[ErrorCodesVersionSwitcher])

  before {
    LogCollector.clear[this.type]
  }

  "ErrorFactories" should {
    "return sqlTransientException" in {
      val failureReason = "some db transient failure"
      val someSqlTransientException = new SQLTransientException(failureReason)
      assertV2Error(
        SelfServiceErrorCodeFactories.sqlTransientException(someSqlTransientException)
      )(
        expectedCode = Code.UNAVAILABLE,
        expectedMessage =
          s"INDEX_DB_SQL_TRANSIENT_ERROR(1,$truncatedCorrelationId): Processing the request failed due to a transient database error: $failureReason",
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
          "INDEX_DB_SQL_TRANSIENT_ERROR(1,cor-id-1): Processing the request failed due to a transient database error: some db transient failure",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return sqlNonTransientException" in {
      val failureReason = "some db non-transient failure"
      assertV2Error(
        SelfServiceErrorCodeFactories
          .sqlNonTransientException(new SQLNonTransientException(failureReason))
      )(
        expectedCode = Code.INTERNAL,
        expectedMessage =
          s"An error occurred. Please contact the operator and inquire about the request $originalCorrelationId",
        expectedDetails = Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo),
        expectedLogEntry = ExpectedLogEntry(
          Level.ERROR,
          "INDEX_DB_SQL_NON_TRANSIENT_ERROR(4,cor-id-1): Processing the request failed due to a non-transient database error: some db non-transient failure",
          Some(excpectedLocationLogMarkerRegex),
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
          v2_message =
            s"An error occurred. Please contact the operator and inquire about the request $originalCorrelationId",
          v2_details = Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo),
          v2_logEntry = ExpectedLogEntry(
            Level.ERROR,
            "LEDGER_API_INTERNAL_ERROR(4,cor-id-1): some message: Exception: message123",
            Some(
              "\\{err-context: \"\\{throwableO=Some\\(java.lang.Exception: message123\\), location=ErrorFactories.scala:\\d+\\}\"\\}"
            ),
          ),
        )
      }

      "return bufferFul" in {
        assertVersionedStatus(
          _.bufferFull("Some buffer is full")(contextualizedErrorLogger)
        )(
          v1_code = Code.RESOURCE_EXHAUSTED,
          v1_message = "Ingress buffer is full",
          v1_details = Seq(errorDetails),
          v2_code = Code.ABORTED,
          v2_message =
            s"PARTICIPANT_BACKPRESSURE(2,$truncatedCorrelationId): The participant is overloaded: Some buffer is full",
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
            "PARTICIPANT_BACKPRESSURE(2,cor-id-1): The participant is overloaded: Some buffer is full",
            Some(
              "\\{err-context: \"\\{reason=Some buffer is full, location=ErrorFactories.scala:\\d+\\}\"\\}"
            ),
          ),
        )
      }

      "return queueClosed" in {
        assertVersionedStatus(
          _.SubmissionQueueErrors.queueClosed("Some service")(
            contextualizedErrorLogger = contextualizedErrorLogger
          )
        )(
          v1_code = Code.ABORTED,
          v1_message = "Queue closed",
          v1_details = Seq(errorDetails),
          v2_code = Code.UNAVAILABLE,
          v2_message =
            s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): Some service has been shut down.",
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
            "SERVICE_NOT_RUNNING(1,cor-id-1): Some service has been shut down.",
            Some(
              "\\{err-context: \"\\{service_name=Some service, location=ErrorFactories.scala:\\d+\\}\"\\}"
            ),
          ),
        )
      }

      "return timeout" in {
        assertVersionedStatus(
          _.SubmissionQueueErrors.timedOutOnAwaitingForCommandCompletion()(
            contextualizedErrorLogger = contextualizedErrorLogger
          )
        )(
          v1_code = Code.ABORTED,
          v1_message = "Timeout",
          v1_details = Seq(errorDetails),
          v2_code = Code.DEADLINE_EXCEEDED,
          v2_message =
            s"REQUEST_TIME_OUT(3,$truncatedCorrelationId): Timed out while awaiting for a completion corresponding to a command submission.",
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
            "REQUEST_TIME_OUT(3,cor-id-1): Timed out while awaiting for a completion corresponding to a command submission.",
            Some(excpectedLocationLogMarkerRegex),
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
          v2_message =
            s"An error occurred. Please contact the operator and inquire about the request cor-id-12345679",
          v2_details = Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo),
          v2_logEntry = ExpectedLogEntry(
            Level.ERROR,
            "LEDGER_API_INTERNAL_ERROR(4,cor-id-1): Missing status in completion response.",
            Some("\\{err-context: \"\\{throwableO=None, location=ErrorFactories.scala:\\d+\\}\"\\}"),
          ),
        )

      }

    }

    "return packageNotFound" in {
      assertVersionedError(_.packageNotFound("packageId123"))(
        v1_code = Code.NOT_FOUND,
        v1_message = "",
        v1_details = Seq.empty,
        v2_code = Code.NOT_FOUND,
        v2_message = s"PACKAGE_NOT_FOUND(11,$truncatedCorrelationId): Could not find package.",
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
          "PACKAGE_NOT_FOUND(11,cor-id-1): Could not find package.",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return the a versioned service internal error" in {
      assertVersionedError(_.versionServiceInternalError("message123"))(
        v1_code = Code.INTERNAL,
        v1_message = "message123",
        v1_details = Seq.empty,
        v2_code = Code.INTERNAL,
        v2_message =
          s"An error occurred. Please contact the operator and inquire about the request $originalCorrelationId",
        v2_details = Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo),
        v2_logEntry = ExpectedLogEntry(
          Level.ERROR,
          "LEDGER_API_INTERNAL_ERROR(4,cor-id-1): message123",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return the configurationEntryRejected" in {
      assertVersionedError(_.configurationEntryRejected("message123", None))(
        v1_code = Code.ABORTED,
        v1_message = "message123",
        v1_details = Seq.empty,
        v2_code = Code.FAILED_PRECONDITION,
        v2_message = s"CONFIGURATION_ENTRY_REJECTED(9,$truncatedCorrelationId): message123",
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "CONFIGURATION_ENTRY_REJECTED",
            Map("category" -> "9", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          "CONFIGURATION_ENTRY_REJECTED(9,cor-id-1): message123",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a transactionNotFound error" in {
      assertVersionedError(_.transactionNotFound(Ref.TransactionId.assertFromString("tId")))(
        v1_code = Code.NOT_FOUND,
        v1_message = "Transaction not found, or not visible.",
        v1_details = Seq.empty,
        v2_code = Code.NOT_FOUND,
        v2_message =
          s"TRANSACTION_NOT_FOUND(11,$truncatedCorrelationId): Transaction not found, or not visible.",
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
          "TRANSACTION_NOT_FOUND(11,cor-id-1): Transaction not found, or not visible.",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return the DuplicateCommandException" in {
      assertVersionedError(_.duplicateCommandException(None))(
        v1_code = Code.ALREADY_EXISTS,
        v1_message = "Duplicate command",
        v1_details = Seq(definiteAnswers(false)),
        v2_code = Code.ALREADY_EXISTS,
        v2_message =
          s"DUPLICATE_COMMAND(10,$truncatedCorrelationId): A command with the given command id has already been successfully processed",
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "DUPLICATE_COMMAND",
            Map("category" -> "10", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          "DUPLICATE_COMMAND(10,cor-id-1): A command with the given command id has already been successfully processed",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a permissionDenied error" in {
      assertVersionedError(_.permissionDenied("some cause"))(
        v1_code = Code.PERMISSION_DENIED,
        v1_message = "",
        v1_details = Seq.empty,
        v2_code = Code.PERMISSION_DENIED,
        v2_message =
          s"An error occurred. Please contact the operator and inquire about the request $originalCorrelationId",
        v2_details = Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo),
        v2_logEntry = ExpectedLogEntry(
          Level.WARN,
          "PERMISSION_DENIED(7,cor-id-1): some cause",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a isTimeoutUnknown_wasAborted error" in {
      assertVersionedError(
        _.isTimeoutUnknown_wasAborted("message123", definiteAnswer = Some(false))
      )(
        v1_code = Code.ABORTED,
        v1_message = "message123",
        v1_details = Seq(definiteAnswers(false)),
        v2_code = Code.DEADLINE_EXCEEDED,
        v2_message = s"REQUEST_TIME_OUT(3,$truncatedCorrelationId): message123",
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
          "REQUEST_TIME_OUT(3,cor-id-1): message123",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a nonHexOffset error" in {
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
        v2_message =
          s"NON_HEXADECIMAL_OFFSET(8,$truncatedCorrelationId): Offset in fieldName123 not specified in hexadecimal: offsetValue123: message123",
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail("NON_HEXADECIMAL_OFFSET", Map("category" -> "8")),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          "NON_HEXADECIMAL_OFFSET(8,cor-id-1): Offset in fieldName123 not specified in hexadecimal: offsetValue123: message123",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return an offsetAfterLedgerEnd error" in {
      val expectedMessage = s"Absolute offset (AABBCC) is after ledger end (E)"
      assertVersionedError(_.offsetAfterLedgerEnd("Absolute", "AABBCC", "E"))(
        v1_code = Code.OUT_OF_RANGE,
        v1_message = expectedMessage,
        v1_details = Seq.empty,
        v2_code = Code.OUT_OF_RANGE,
        v2_message = s"OFFSET_AFTER_LEDGER_END(12,$truncatedCorrelationId): $expectedMessage",
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "OFFSET_AFTER_LEDGER_END",
            Map("category" -> "12", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          "OFFSET_AFTER_LEDGER_END(12,cor-id-1): Absolute offset (AABBCC) is after ledger end (E)",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a offsetOutOfRange error" in {
      assertVersionedError(_.offsetOutOfRange(None)("message123"))(
        v1_code = Code.INVALID_ARGUMENT,
        v1_message = "Invalid argument: message123",
        v1_details = Seq.empty,
        v2_code = Code.FAILED_PRECONDITION,
        v2_message = s"OFFSET_OUT_OF_RANGE(9,$truncatedCorrelationId): message123",
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "OFFSET_OUT_OF_RANGE",
            Map("category" -> "9", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          "OFFSET_OUT_OF_RANGE(9,cor-id-1): message123",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return an unauthenticatedMissingJwtToken error" in {
      assertVersionedError(_.unauthenticatedMissingJwtToken())(
        v1_code = Code.UNAUTHENTICATED,
        v1_message = "",
        v1_details = Seq.empty,
        v2_code = Code.UNAUTHENTICATED,
        v2_message =
          s"An error occurred. Please contact the operator and inquire about the request $originalCorrelationId",
        v2_details = Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo),
        v2_logEntry = ExpectedLogEntry(
          Level.WARN,
          "UNAUTHENTICATED(6,cor-id-1): The command is missing a JWT token",
          Some(excpectedLocationLogMarkerRegex),
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
        v2_message =
          s"An error occurred. Please contact the operator and inquire about the request $originalCorrelationId",
        v2_details = Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo),
        v2_logEntry = ExpectedLogEntry(
          Level.ERROR,
          "INTERNAL_AUTHORIZATION_ERROR(4,cor-id-1): nothing security sensitive in here",
          Some(excpectedLocationLogMarkerRegex),
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
        assertVersionedError(_.missingLedgerConfig(legacyErrorCode)(definiteAnswer))(
          v1_code = legacyErrorCode,
          v1_message = "The ledger configuration is not available.",
          v1_details = expectedDetails,
          v2_code = Code.NOT_FOUND,
          v2_message =
            s"LEDGER_CONFIGURATION_NOT_FOUND(11,$truncatedCorrelationId): The ledger configuration could not be retrieved.",
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "LEDGER_CONFIGURATION_NOT_FOUND",
              Map("category" -> "11", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            "LEDGER_CONFIGURATION_NOT_FOUND(11,cor-id-1): The ledger configuration could not be retrieved.",
            Some(excpectedLocationLogMarkerRegex),
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
        v2_message =
          s"INVALID_DEDUPLICATION_PERIOD(9,$truncatedCorrelationId): The submitted command had an invalid deduplication period: $errorDetailMessage",
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
          "INVALID_DEDUPLICATION_PERIOD(9,cor-id-1): The submitted command had an invalid deduplication period: message",
          Some(
            "\\{err-context: \"\\{max_deduplication_duration=PT5S, location=ErrorFactories.scala:\\d+\\}\"\\}"
          ),
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
        assertVersionedError(_.invalidField(fieldName, "my message", definiteAnswer))(
          v1_code = Code.INVALID_ARGUMENT,
          v1_message = "Invalid field " + fieldName + ": my message",
          v1_details = expectedDetails,
          v2_code = Code.INVALID_ARGUMENT,
          v2_message =
            s"INVALID_FIELD(8,$truncatedCorrelationId): The submitted command has a field with invalid value: Invalid field $fieldName: my message",
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "INVALID_FIELD",
              Map("category" -> "8", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            "INVALID_FIELD(8,cor-id-1): The submitted command has a field with invalid value: Invalid field my field: my message",
            Some(excpectedLocationLogMarkerRegex),
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
        assertVersionedError(
          _.ledgerIdMismatch(LedgerId("expected"), LedgerId("received"), definiteAnswer)
        )(
          v1_code = Code.NOT_FOUND,
          v1_message = "Ledger ID 'received' not found. Actual Ledger ID is 'expected'.",
          v1_details = expectedDetails,
          v2_code = Code.NOT_FOUND,
          v2_message =
            s"LEDGER_ID_MISMATCH(11,$truncatedCorrelationId): Ledger ID 'received' not found. Actual Ledger ID is 'expected'.",
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "LEDGER_ID_MISMATCH",
              Map("category" -> "11", "definite_answer" -> "true"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            "LEDGER_ID_MISMATCH(11,cor-id-1): Ledger ID 'received' not found. Actual Ledger ID is 'expected'.",
            Some(excpectedLocationLogMarkerRegex),
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
      assertVersionedError(_.participantPrunedDataAccessed("my message"))(
        v1_code = Code.NOT_FOUND,
        v1_message = "my message",
        v1_details = Seq.empty,
        v2_code = Code.FAILED_PRECONDITION,
        v2_message = s"PARTICIPANT_PRUNED_DATA_ACCESSED(9,$truncatedCorrelationId): my message",
        v2_details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "PARTICIPANT_PRUNED_DATA_ACCESSED",
            Map("category" -> "9", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        v2_logEntry = ExpectedLogEntry(
          Level.INFO,
          "PARTICIPANT_PRUNED_DATA_ACCESSED(9,cor-id-1): my message",
          Some(excpectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a trackerFailure error" in {
      assertVersionedError(_.trackerFailure("message123"))(
        v1_code = Code.INTERNAL,
        v1_message = "message123",
        v1_details = Seq.empty,
        v2_code = Code.INTERNAL,
        v2_message =
          s"An error occurred. Please contact the operator and inquire about the request $originalCorrelationId",
        v2_details = Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo),
        v2_logEntry = ExpectedLogEntry(
          Level.ERROR,
          "LEDGER_API_INTERNAL_ERROR(4,cor-id-1): message123",
          Some("\\{err-context: \"\\{throwableO=None, location=ErrorFactories.scala:\\d+\\}\"\\}"),
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
        assertVersionedError(_.serviceNotRunning(serviceName)(definiteAnswer))(
          v1_code = Code.UNAVAILABLE,
          v1_message = s"$serviceName has been shut down.",
          v1_details = expectedDetails,
          v2_code = Code.UNAVAILABLE,
          v2_message =
            s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): $serviceName has been shut down.",
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
            "SERVICE_NOT_RUNNING(1,cor-id-1): Some API Service has been shut down.",
            Some(
              "\\{err-context: \"\\{service_name=Some API Service, location=ErrorFactories.scala:\\d+\\}\"\\}"
            ),
          ),
        )
      }
    }

    "return a serviceIsBeingReset error" in {
      val serviceName = "Some API Service"
      val someLegacyStatusCode = Code.CANCELLED

      assertVersionedError(_.serviceIsBeingReset(someLegacyStatusCode.value())(serviceName))(
        v1_code = someLegacyStatusCode,
        v1_message = s"$serviceName is currently being reset.",
        v1_details = Seq.empty,
        v2_code = Code.UNAVAILABLE,
        v2_message =
          s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): $serviceName is currently being reset.",
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
          "SERVICE_NOT_RUNNING(1,cor-id-1): Some API Service is currently being reset.",
          Some(
            "\\{err-context: \"\\{service_name=Some API Service, location=ErrorFactories.scala:\\d+\\}\"\\}"
          ),
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
        assertVersionedError(_.missingField(fieldName, definiteAnswer))(
          v1_code = Code.INVALID_ARGUMENT,
          v1_message = "Missing field: " + fieldName,
          v1_details = expectedDetails,
          v2_code = Code.INVALID_ARGUMENT,
          v2_message =
            s"MISSING_FIELD(8,$truncatedCorrelationId): The submitted command is missing a mandatory field: $fieldName",
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "MISSING_FIELD",
              Map("category" -> "8", "definite_answer" -> "false", "field_name" -> fieldName),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            "MISSING_FIELD(8,cor-id-1): The submitted command is missing a mandatory field: my field",
            Some(
              "\\{err-context: \"\\{field_name=my field, location=ErrorFactories.scala:\\d+\\}\"\\}"
            ),
          ),
        )
      }
    }

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
          v2_message =
            s"INVALID_ARGUMENT(8,$truncatedCorrelationId): The submitted command has invalid arguments: my message",
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "INVALID_ARGUMENT",
              Map("category" -> "8", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            "INVALID_ARGUMENT(8,cor-id-1): The submitted command has invalid arguments: my message",
            Some(excpectedLocationLogMarkerRegex),
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
          v2_message =
            s"INVALID_ARGUMENT(8,$truncatedCorrelationId): The submitted command has invalid arguments: my message",
          v2_details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "INVALID_ARGUMENT",
              Map("category" -> "8", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
          ),
          v2_logEntry = ExpectedLogEntry(
            Level.INFO,
            "INVALID_ARGUMENT(8,cor-id-1): The submitted command has invalid arguments: my message",
            Some(excpectedLocationLogMarkerRegex),
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
  )(implicit dummyImplicit: DummyImplicit, dummyImplicit2: DummyImplicit): Unit = {
    val status = StatusProto.fromThrowable(statusRuntimeException)
    status.getCode shouldBe expectedCode.value()
    status.getMessage shouldBe expectedMessage
    val details = status.getDetailsList.asScala.toSeq
    val _ = ErrorDetails.from(details) should contain theSameElementsAs expectedDetails
    val actualLogs: Seq[LogCollector.Entry] = LogCollector.readAsEntries[this.type, this.type]
    actualLogs should have size 1
    assertLogEntry(actualLogs.head, expectedLogEntry)
  }
}
