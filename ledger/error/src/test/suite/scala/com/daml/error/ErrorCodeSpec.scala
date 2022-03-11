// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import ch.qos.logback.classic.Level
import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.testpackage.SeriousError
import com.daml.logging.LoggingContext
import com.daml.platform.testing.LogCollector.ThrowableEntry
import com.daml.platform.testing.{LogCollector, LogCollectorAssertions}
import com.google.rpc.Status
import io.grpc.Status.Code
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class ErrorCodeSpec
    extends AnyFreeSpec
    with Matchers
    with BeforeAndAfter
    with LogCollectorAssertions
    with ErrorAssertionsWithLogCollectorAssertions {

  private val contextualizedErrorLoggerF =
    (correlationId: Option[String]) =>
      DamlContextualizedErrorLogger.forTesting(getClass, correlationId)

  before {
    LogCollector.clear[this.type]
  }

  object FooErrorCodeSecuritySensitive
      extends ErrorCode(
        "FOO_ERROR_CODE_SECURITY_SENSITIVE",
        ErrorCategory.SystemInternalAssumptionViolated,
      )(ErrorClass.root())

  object FooErrorCode
      extends ErrorCode("FOO_ERROR_CODE", ErrorCategory.InvalidIndependentOfSystemState)(
        ErrorClass.root()
      )

  classOf[ErrorCode].getSimpleName - {

    "meet test preconditions" in {
      FooErrorCodeSecuritySensitive.category.securitySensitive shouldBe true
      FooErrorCode.category.securitySensitive shouldBe false
      FooErrorCodeSecuritySensitive.category.grpcCode shouldBe Some(Code.INTERNAL)
      FooErrorCode.category.grpcCode shouldBe Some(Code.INVALID_ARGUMENT)
    }

    "create correct message" in {
      FooErrorCode.toMsg(
        cause = "cause123",
        correlationId = Some("123correlationId"),
      ) shouldBe "FOO_ERROR_CODE(8,123corre): cause123"
      FooErrorCode.toMsg(
        cause = "cause123",
        correlationId = None,
      ) shouldBe "FOO_ERROR_CODE(8,0): cause123"
      FooErrorCode.toMsg(
        cause = "x" * ErrorCode.MaxCauseLogLength * 2,
        correlationId = Some("123correlationId"),
      ) shouldBe s"FOO_ERROR_CODE(8,123corre): ${"x" * ErrorCode.MaxCauseLogLength}..."
      FooErrorCodeSecuritySensitive.toMsg(
        cause = "cause123",
        correlationId = Some("123correlationId"),
      ) shouldBe "FOO_ERROR_CODE_SECURITY_SENSITIVE(4,123corre): cause123"
    }

    "create a minimal grpc status and exception" in {
      class FooErrorMinimal(override val code: ErrorCode) extends BaseError {
        override val cause: String = "cause123"
      }
      val errorLoggerSmall = DamlContextualizedErrorLogger.forClass(
        clazz = getClass,
        correlationId = None,
        loggingContext = LoggingContext.empty,
      )
      val testedErrorCode = FooErrorCode
      case class TestedError() extends FooErrorMinimal(testedErrorCode)
      val details = Seq(
        ErrorDetails
          .ErrorInfoDetail(
            testedErrorCode.id,
            Map("category" -> testedErrorCode.category.asInt.toString),
          )
      )
      val expected = Status
        .newBuilder()
        .setMessage("FOO_ERROR_CODE(8,0): cause123")
        .setCode(Code.INVALID_ARGUMENT.value())
        .addAllDetails(details.map(_.toRpcAny).asJava)
        .build()
      val testedError = TestedError()

      assertStatus(
        actual = testedErrorCode.asGrpcStatus(testedError)(errorLoggerSmall),
        expected = expected,
      )
      assertError(
        actual = testedErrorCode.asGrpcError(testedError)(errorLoggerSmall),
        expectedStatusCode = testedErrorCode.category.grpcCode.get,
        expectedMessage = "FOO_ERROR_CODE(8,0): cause123",
        expectedDetails = details,
      )
    }

    "create a big grpc status and exception" - {
      class FooErrorBig(override val code: ErrorCode) extends BaseError {
        override val cause: String = "cause123"

        override def retryable: Option[ErrorCategoryRetry] = Some(
          ErrorCategoryRetry(duration = 123.seconds + 456.milliseconds)
        )

        override def resources: Seq[(ErrorResource, String)] =
          super.resources ++
            Seq[(ErrorResource, String)](
              ErrorResource.CommandId -> "commandId1",
              ErrorResource.CommandId -> "commandId2",
              ErrorResource.Party -> "party1",
            )

        override def context: Map[String, String] =
          super.context ++ Map(
            "contextKey1" -> "contextValue1",
            "kkk????" -> "keyWithInvalidCharacters",
          )

        override def definiteAnswerO: Option[Boolean] = Some(false)

        override def throwableO: Option[Throwable] =
          Some(new RuntimeException("runtimeException123"))
      }

      val errorLoggerBig = DamlContextualizedErrorLogger.forClass(
        clazz = getClass,
        correlationId = Some("123correlationId"),
        loggingContext = LoggingContext(
          "loggingEntryKey" -> "loggingEntryValue"
        ),
      )
      val requestInfo = ErrorDetails.RequestInfoDetail("123correlationId")
      val retryInfo = ErrorDetails.RetryInfoDetail(123.seconds + 456.milliseconds)

      def getDetails(tested: ErrorCode) = Seq(
        ErrorDetails
          .ErrorInfoDetail(
            tested.id,
            Map(
              "category" -> tested.category.asInt.toString,
              "definite_answer" -> "false",
              "loggingEntryKey" -> "'loggingEntryValue'",
              "contextKey1" -> "contextValue1",
              "kkk" -> "keyWithInvalidCharacters",
            ),
          ),
        requestInfo,
        retryInfo,
        ErrorDetails.ResourceInfoDetail(name = "commandId1", typ = "COMMAND_ID"),
        ErrorDetails.ResourceInfoDetail(name = "commandId2", typ = "COMMAND_ID"),
        ErrorDetails.ResourceInfoDetail(name = "party1", typ = "PARTY"),
      )

      "not security sensitive" in {
        val testedErrorCode = FooErrorCode
        val details = getDetails(testedErrorCode)
        case class TestedError() extends FooErrorBig(testedErrorCode)

        val expectedStatus = Status
          .newBuilder()
          .setMessage("FOO_ERROR_CODE(8,123corre): cause123")
          .setCode(testedErrorCode.category.grpcCode.get.value())
          .addAllDetails(details.map(_.toRpcAny).asJava)
          .build()
        val testedError = TestedError()

        assertStatus(
          actual = testedErrorCode.asGrpcStatus(testedError)(errorLoggerBig),
          expected = expectedStatus,
        )
        assertError(
          actual = testedErrorCode.asGrpcError(testedError)(errorLoggerBig),
          expectedStatusCode = testedErrorCode.category.grpcCode.get,
          expectedMessage = "FOO_ERROR_CODE(8,123corre): cause123",
          expectedDetails = details,
        )
      }

      "security sensitive" in {
        val testedErrorCode = FooErrorCodeSecuritySensitive
        case class FooError() extends FooErrorBig(testedErrorCode)
        val expectedStatus = Status
          .newBuilder()
          .setMessage(
            "An error occurred. Please contact the operator and inquire about the request 123correlationId"
          )
          .setCode(testedErrorCode.category.grpcCode.get.value())
          .addDetails(requestInfo.toRpcAny)
          .addDetails(retryInfo.toRpcAny)
          .build()
        val testedError = FooError()
        testedError.logWithContext(Map.empty)(errorLoggerBig)

        assertSingleLogEntry(
          actual = LogCollector.readAsEntries[this.type, this.type],
          expectedLogLevel = Level.ERROR,
          expectedMsg = "FOO_ERROR_CODE_SECURITY_SENSITIVE(4,123corre): cause123",
          expectedMarkerAsString =
            """{loggingEntryKey: "loggingEntryValue", err-context: "{contextKey1=contextValue1, kkk????=keyWithInvalidCharacters, location=ErrorCodeSpec.scala:<line-number>}"}""",
          expectedThrowableEntry = Some(
            ThrowableEntry(
              className = "java.lang.RuntimeException",
              message = "runtimeException123",
            )
          ),
        )
        assertStatus(
          actual = testedErrorCode.asGrpcStatus(testedError)(errorLoggerBig),
          expected = expectedStatus,
        )
        assertError(
          actual = testedErrorCode.asGrpcError(testedError)(errorLoggerBig),
          expectedStatusCode = testedErrorCode.category.grpcCode.get,
          expectedMessage =
            "An error occurred. Please contact the operator and inquire about the request 123correlationId",
          expectedDetails = Seq(requestInfo, retryInfo),
        )
      }

    }

    "create a grpc status and exception for input exceeding details size limits" in {
      class FooErrorBig(override val code: ErrorCode) extends BaseError {
        override val cause: String = "cause123"

        override def context: Map[String, String] =
          super.context ++ Map(
            ("y" * ErrorCode.MaxContentBytes) -> ("y" * ErrorCode.MaxContentBytes)
          )

        override def retryable: Option[ErrorCategoryRetry] = Some(
          ErrorCategoryRetry(duration = 123.seconds + 456.milliseconds)
        )

        override def resources: Seq[(ErrorResource, String)] =
          super.resources ++
            Seq[(ErrorResource, String)](
              ErrorResource.CommandId -> "commandId1",
              ErrorResource.CommandId -> "commandId2",
              ErrorResource.Party -> "party1",
              ErrorResource.Party -> ("x" * ErrorCode.MaxContentBytes),
            )

        override def definiteAnswerO: Option[Boolean] = Some(false)
      }
      val errorLoggerOversized = DamlContextualizedErrorLogger.forClass(
        clazz = getClass,
        correlationId = Some("123correlationId"),
        loggingContext = LoggingContext(
          "loggingEntryKey" -> "loggingEntryValue",
          "loggingEntryValueTooBig" -> ("x" * ErrorCode.MaxContentBytes),
          ("x" * ErrorCode.MaxContentBytes) -> "loggingEntryKeyTooBig",
        ),
      )
      val requestInfo = ErrorDetails.RequestInfoDetail("123correlationId")
      val retryInfo = ErrorDetails.RetryInfoDetail(123.seconds + 456.milliseconds)

      val testedErrorCode = FooErrorCode
      case class TestedError() extends FooErrorBig(FooErrorCode)
      val testedError = TestedError()

      val expectedDetails = Seq(
        ErrorDetails
          .ErrorInfoDetail(
            testedErrorCode.id,
            Map(
              "category" -> testedErrorCode.category.asInt.toString,
              "definite_answer" -> "false",
              "loggingEntryKey" -> "'loggingEntryValue'",
              "loggingEntryValueTooBig" -> ("'" + "x" * 473 + "..."),
              ("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") -> "'loggingEntryKeyTooBig'",
              ("yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy") -> ("y" * 1317 + "..."),
            ),
          ),
        requestInfo,
        retryInfo,
        ErrorDetails.ResourceInfoDetail(name = "commandId1", typ = "COMMAND_ID"),
        ErrorDetails.ResourceInfoDetail(name = "commandId2", typ = "COMMAND_ID"),
        ErrorDetails.ResourceInfoDetail(name = "party1", typ = "PARTY"),
      )
      val expectedMessage = "FOO_ERROR_CODE(8,123corre): cause123"
      val expectedStatus = Status
        .newBuilder()
        .setMessage(expectedMessage)
        .setCode(testedErrorCode.category.grpcCode.get.value())
        .addAllDetails(expectedDetails.map(_.toRpcAny).asJava)
        .build()

      assertStatus(
        actual = testedErrorCode.asGrpcStatus(testedError)(errorLoggerOversized),
        expected = expectedStatus,
      )
      assertError(
        actual = testedErrorCode.asGrpcError(testedError)(errorLoggerOversized),
        expectedStatusCode = testedErrorCode.category.grpcCode.get,
        expectedMessage = expectedMessage,
        expectedDetails = expectedDetails,
      )
    }

    "log the error message with the correct markers" in {
      val error = SeriousError.Error(
        "the error argument",
        context = Map("extra-context-key" -> "extra-context-value"),
      )
      val errorLogger = DamlContextualizedErrorLogger.forTesting(getClass, Some("1234567890"))

      error.logWithContext()(errorLogger)

      val actualLogs = LogCollector
        .readAsEntries[this.type, this.type]
      actualLogs.size shouldBe 1
      assertLogEntry(
        actualLogs.head,
        expectedLogLevel = Level.ERROR,
        expectedMsg = "BLUE_SCREEN(4,12345678): the error argument",
        expectedMarkerRegex = Some(
          "\\{err-context: \"\\{extra-context-key=extra-context-value, location=ErrorCodeSpec.scala:\\d+\\}\"\\}"
        ),
      )
    }

    s"truncate the cause size if larger than ${ErrorCode.MaxCauseLogLength}" in {
      val veryLongCause = "o" * (ErrorCode.MaxCauseLogLength * 2)
      val error =
        SeriousError.Error(
          veryLongCause,
          context = Map("extra-context-key" -> "extra-context-value"),
        )

      error.logWithContext()(contextualizedErrorLoggerF(None))

      val expectedErrorLog = "BLUE_SCREEN(4,0): " + ("o" * ErrorCode.MaxCauseLogLength + "...")
      val actualLogs = LogCollector.read[this.type, this.type]
      actualLogs shouldBe Seq(Level.ERROR -> expectedErrorLog)
    }

  }

}
