// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.server.api.validation

import java.sql.{SQLNonTransientException, SQLTransientException}
import java.time.Duration
import java.util.regex.Pattern

import ch.qos.logback.classic.Level
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.definitions.LedgerApiErrors.RequestValidation.InvalidDeduplicationPeriodField.ValidMaxDeduplicationFieldKey
import com.daml.error.utils.ErrorDetails
import com.daml.error.{
  ContextualizedErrorLogger,
  DamlContextualizedErrorLogger,
  ErrorAssertionsWithLogCollectorAssertions,
}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.offset.Offset
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
import scala.concurrent.duration._

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
    with ErrorAssertionsWithLogCollectorAssertions {

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
  private val errorFactoriesSpecLocationLogMarkerRegex =
    "\\{err-context: \"\\{location=ErrorFactoriesSpec.scala:\\d+\\}\"\\}"
  private val expectedInternalErrorMessage =
    s"An error occurred. Please contact the operator and inquire about the request $originalCorrelationId"
  private val expectedInternalErrorDetails =
    Seq[ErrorDetails.ErrorDetail](expectedCorrelationIdRequestInfo)

  private val tested = ErrorFactories()

  before {
    LogCollector.clear[this.type]
  }

  "ErrorFactories" should {
    val errorFactories = ErrorFactories()

    "return sqlTransientException" in {
      val failureReason = "some db transient failure"
      val someSqlTransientException = new SQLTransientException(failureReason)
      val msg =
        s"INDEX_DB_SQL_TRANSIENT_ERROR(1,$truncatedCorrelationId): Processing the request failed due to a transient database error: $failureReason"
      assertError(
        errorFactories.sqlTransientException(someSqlTransientException)
      )(
        code = Code.UNAVAILABLE,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1.second),
          ErrorDetails.ErrorInfoDetail(
            "INDEX_DB_SQL_TRANSIENT_ERROR",
            Map("category" -> "1", "definite_answer" -> "false"),
          ),
        ),
        logEntry = ExpectedLogEntry(
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
      assertError(
        errorFactories
          .sqlNonTransientException(new SQLNonTransientException(failureReason))
      )(
        code = Code.INTERNAL,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logEntry = ExpectedLogEntry(
          Level.ERROR,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "TrackerErrors" should {
      "return failedToEnqueueCommandSubmission" in {
        val t = new Exception("message123")
        assertStatus(
          errorFactories.SubmissionQueueErrors.failedToEnqueueCommandSubmission("some message")(t)(
            contextualizedErrorLogger
          )
        )(
          code = Code.INTERNAL,
          message = expectedInternalErrorMessage,
          details = expectedInternalErrorDetails,
          logEntry = ExpectedLogEntry(
            Level.ERROR,
            s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): some message: Exception: message123",
            expectedMarkerRegex("throwableO=Some(java.lang.Exception: message123)"),
          ),
        )
      }

      "return bufferFul" in {
        val msg =
          s"PARTICIPANT_BACKPRESSURE(2,$truncatedCorrelationId): The participant is overloaded: Some buffer is full"
        assertStatus(
          errorFactories.bufferFull("Some buffer is full")(contextualizedErrorLogger)
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
              ),
            ),
            expectedCorrelationIdRequestInfo,
            ErrorDetails.RetryInfoDetail(1.second),
          ),
          logEntry = ExpectedLogEntry(
            Level.WARN,
            msg,
            expectedMarkerRegex("reason=Some buffer is full"),
          ),
        )
      }

      "return queueClosed" in {
        val msg =
          s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): Some service has been shut down."
        assertStatus(
          errorFactories.SubmissionQueueErrors.queueClosed("Some service")(
            contextualizedErrorLogger = contextualizedErrorLogger
          )
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
              ),
            ),
            expectedCorrelationIdRequestInfo,
            ErrorDetails.RetryInfoDetail(1.second),
          ),
          logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            expectedMarkerRegex("service_name=Some service"),
          ),
        )
      }

      "return timeout" in {
        val msg =
          s"REQUEST_TIME_OUT(3,$truncatedCorrelationId): Timed out while awaiting for a completion corresponding to a command submission."
        assertStatus(
          errorFactories.SubmissionQueueErrors.timedOutOnAwaitingForCommandCompletion()(
            contextualizedErrorLogger = contextualizedErrorLogger
          )
        )(
          code = Code.DEADLINE_EXCEEDED,
          message = msg,
          details = Seq[ErrorDetails.ErrorDetail](
            ErrorDetails.ErrorInfoDetail(
              "REQUEST_TIME_OUT",
              Map("category" -> "3", "definite_answer" -> "false"),
            ),
            expectedCorrelationIdRequestInfo,
            ErrorDetails.RetryInfoDetail(1.second),
          ),
          logEntry = ExpectedLogEntry(
            Level.INFO,
            msg,
            Some(expectedLocationLogMarkerRegex),
          ),
        )
      }
      "return noStatusInResponse" in {
        assertStatus(
          errorFactories.SubmissionQueueErrors.noStatusInCompletionResponse()(
            contextualizedErrorLogger = contextualizedErrorLogger
          )
        )(
          code = Code.INTERNAL,
          message = expectedInternalErrorMessage,
          details = expectedInternalErrorDetails,
          logEntry = ExpectedLogEntry(
            Level.ERROR,
            s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): Missing status in completion response.",
            expectedMarkerRegex("throwableO=None"),
          ),
        )

      }

    }

    "return packageNotFound" in {
      val msg = s"PACKAGE_NOT_FOUND(11,$truncatedCorrelationId): Could not find package."
      assertError(errorFactories.packageNotFound("packageId123"))(
        code = Code.NOT_FOUND,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "PACKAGE_NOT_FOUND",
            Map("category" -> "11", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.ResourceInfoDetail("PACKAGE", "packageId123"),
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return the a versioned service internal error" in {
      assertError(errorFactories.versionServiceInternalError("message123"))(
        code = Code.INTERNAL,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logEntry = ExpectedLogEntry(
          Level.ERROR,
          s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): message123",
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return the configurationEntryRejected" in {
      val msg = s"CONFIGURATION_ENTRY_REJECTED(9,$truncatedCorrelationId): message123"
      assertError(errorFactories.configurationEntryRejected("message123"))(
        code = Code.FAILED_PRECONDITION,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "CONFIGURATION_ENTRY_REJECTED",
            Map("category" -> "9", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a transactionNotFound error" in {
      val msg =
        s"TRANSACTION_NOT_FOUND(11,$truncatedCorrelationId): Transaction not found, or not visible."
      assertError(errorFactories.transactionNotFound(Ref.TransactionId.assertFromString("tId")))(
        code = Code.NOT_FOUND,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "TRANSACTION_NOT_FOUND",
            Map("category" -> "11", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.ResourceInfoDetail("TRANSACTION_ID", "tId"),
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return the DuplicateCommandException" in {
      val msg =
        s"DUPLICATE_COMMAND(10,$truncatedCorrelationId): A command with the given command id has already been successfully processed"
      assertError(errorFactories.duplicateCommandException(None))(
        code = Code.ALREADY_EXISTS,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "DUPLICATE_COMMAND",
            Map("category" -> "10", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a permissionDenied error" in {
      assertError(errorFactories.permissionDenied("some cause"))(
        code = Code.PERMISSION_DENIED,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logEntry = ExpectedLogEntry(
          Level.WARN,
          s"PERMISSION_DENIED(7,$truncatedCorrelationId): some cause",
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a isTimeoutUnknown_wasAborted error" in {
      val msg = s"REQUEST_TIME_OUT(3,$truncatedCorrelationId): message123"
      assertError(
        errorFactories.isTimeoutUnknown_wasAborted("message123", definiteAnswer = Some(false))
      )(
        code = Code.DEADLINE_EXCEEDED,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "REQUEST_TIME_OUT",
            Map("category" -> "3", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1.second),
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a nonHexOffset error" in {
      val msg =
        s"NON_HEXADECIMAL_OFFSET(8,$truncatedCorrelationId): Offset in fieldName123 not specified in hexadecimal: offsetValue123: message123"
      assertError(
        errorFactories.nonHexOffset(
          fieldName = "fieldName123",
          offsetValue = "offsetValue123",
          message = "message123",
        )
      )(
        code = Code.INVALID_ARGUMENT,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail("NON_HEXADECIMAL_OFFSET", Map("category" -> "8")),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return an offsetAfterLedgerEnd error" in {
      val expectedMessage = s"Absolute offset (AABBCC) is after ledger end (E)"
      val msg = s"OFFSET_AFTER_LEDGER_END(12,$truncatedCorrelationId): $expectedMessage"
      assertError(
        LedgerApiErrors.RequestValidation.OffsetAfterLedgerEnd
          .Reject("Absolute", "AABBCC", "E")
          .asGrpcError
      )(
        code = Code.OUT_OF_RANGE,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "OFFSET_AFTER_LEDGER_END",
            Map("category" -> "12", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(errorFactoriesSpecLocationLogMarkerRegex),
        ),
      )
    }

    "return a offsetOutOfRange error" in {
      val msg = s"OFFSET_OUT_OF_RANGE(9,$truncatedCorrelationId): message123"
      assertError(errorFactories.offsetOutOfRange("message123"))(
        code = Code.FAILED_PRECONDITION,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "OFFSET_OUT_OF_RANGE",
            Map("category" -> "9", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return an unauthenticatedMissingJwtToken error" in {
      assertError(errorFactories.unauthenticatedMissingJwtToken())(
        code = Code.UNAUTHENTICATED,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logEntry = ExpectedLogEntry(
          Level.WARN,
          s"UNAUTHENTICATED(6,$truncatedCorrelationId): The command is missing a (valid) JWT token",
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return an internalAuthenticationError" in {
      val someSecuritySafeMessage = "nothing security sensitive in here"
      val someThrowable = new RuntimeException("some internal authentication error")
      assertError(
        errorFactories.internalAuthenticationError(someSecuritySafeMessage, someThrowable)
      )(
        code = Code.INTERNAL,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logEntry = ExpectedLogEntry(
          Level.ERROR,
          s"INTERNAL_AUTHORIZATION_ERROR(4,$truncatedCorrelationId): nothing security sensitive in here",
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a missingLedgerConfig error" in {
      val msg =
        s"LEDGER_CONFIGURATION_NOT_FOUND(11,$truncatedCorrelationId): The ledger configuration could not be retrieved."
      assertError(errorFactories.missingLedgerConfig())(
        code = Code.NOT_FOUND,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "LEDGER_CONFIGURATION_NOT_FOUND",
            Map("category" -> "11", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
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
      val maxDeduplicationDuration = Duration.ofSeconds(5)
      val msg =
        s"INVALID_DEDUPLICATION_PERIOD(9,$truncatedCorrelationId): The submitted command had an invalid deduplication period: $errorDetailMessage"
      assertError(
        errorFactories.invalidDeduplicationPeriod(
          message = errorDetailMessage,
          maxDeduplicationDuration = Some(maxDeduplicationDuration),
        )
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
            ),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          expectedMarkerRegex("longest_duration=PT5S"),
        ),
      )
    }

    "return an invalidField error" in {
      val fieldName = "my field"
      val msg =
        s"INVALID_FIELD(8,$truncatedCorrelationId): The submitted command has a field with invalid value: Invalid field $fieldName: my message"
      assertError(errorFactories.invalidField(fieldName, "my message"))(
        code = Code.INVALID_ARGUMENT,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "INVALID_FIELD",
            Map("category" -> "8", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a ledgerIdMismatch error" in {
      val msg =
        s"LEDGER_ID_MISMATCH(11,$truncatedCorrelationId): Ledger ID 'received' not found. Actual Ledger ID is 'expected'."
      assertError(
        errorFactories.ledgerIdMismatch(LedgerId("expected"), LedgerId("received"))
      )(
        code = Code.NOT_FOUND,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "LEDGER_ID_MISMATCH",
            Map("category" -> "11", "definite_answer" -> "true"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
    }

    "return a participantPrunedDataAccessed error" in {
      val msg = s"PARTICIPANT_PRUNED_DATA_ACCESSED(9,$truncatedCorrelationId): my message"
      assertError(
        errorFactories.participantPrunedDataAccessed(
          "my message",
          Offset.fromHexString(Ref.HexString.assertFromString("00")),
        )
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
            ),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          expectedMarkerRegex(s"${LedgerApiErrors.EarliestOffsetMetadataKey}=00"),
        ),
      )
    }

    "return a trackerFailure error" in {
      assertError(errorFactories.trackerFailure("message123"))(
        code = Code.INTERNAL,
        message = expectedInternalErrorMessage,
        details = expectedInternalErrorDetails,
        logEntry = ExpectedLogEntry(
          Level.ERROR,
          s"LEDGER_API_INTERNAL_ERROR(4,$truncatedCorrelationId): message123",
          expectedMarkerRegex("throwableO=None"),
        ),
      )
    }

    "return a serviceNotRunning error" in {
      val serviceName = "Some API Service"

      val msg =
        s"SERVICE_NOT_RUNNING(1,$truncatedCorrelationId): $serviceName has been shut down."
      assertError(errorFactories.serviceNotRunning(serviceName))(
        code = Code.UNAVAILABLE,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "SERVICE_NOT_RUNNING",
            Map("category" -> "1", "definite_answer" -> "false", "service_name" -> serviceName),
          ),
          expectedCorrelationIdRequestInfo,
          ErrorDetails.RetryInfoDetail(1.second),
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          expectedMarkerRegex("service_name=Some API Service"),
        ),
      )
    }

    "return a missingField error" in {
      val fieldName = "my field"

      val msg =
        s"MISSING_FIELD(8,$truncatedCorrelationId): The submitted command is missing a mandatory field: $fieldName"
      assertError(errorFactories.missingField(fieldName))(
        code = Code.INVALID_ARGUMENT,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "MISSING_FIELD",
            Map("category" -> "8", "definite_answer" -> "false", "field_name" -> fieldName),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          expectedMarkerRegex("field_name=my field"),
        ),
      )
    }

    val msg =
      s"INVALID_ARGUMENT(8,$truncatedCorrelationId): The submitted command has invalid arguments: my message"
    "return an invalidArgument error" in {
      assertError(errorFactories.invalidArgument("my message"))(
        code = Code.INVALID_ARGUMENT,
        message = msg,
        details = Seq[ErrorDetails.ErrorDetail](
          ErrorDetails.ErrorInfoDetail(
            "INVALID_ARGUMENT",
            Map("category" -> "8", "definite_answer" -> "false"),
          ),
          expectedCorrelationIdRequestInfo,
        ),
        logEntry = ExpectedLogEntry(
          Level.INFO,
          msg,
          Some(expectedLocationLogMarkerRegex),
        ),
      )
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

  private def assertStatus(status: Status)(
      code: Code,
      message: String,
      details: Seq[ErrorDetails.ErrorDetail],
      logEntry: ExpectedLogEntry,
  ): Unit =
    assertError(io.grpc.protobuf.StatusProto.toStatusRuntimeException(status))(
      code,
      message,
      details,
      logEntry,
    )

  private def assertError(
      statusRuntimeException: StatusRuntimeException
  )(
      code: Code,
      message: String,
      details: Seq[ErrorDetails.ErrorDetail],
      logEntry: ExpectedLogEntry,
  ): Unit =
    assertError[this.type, this.type](
      actual = statusRuntimeException,
      code,
      message,
      details,
      logEntry,
    )
}
