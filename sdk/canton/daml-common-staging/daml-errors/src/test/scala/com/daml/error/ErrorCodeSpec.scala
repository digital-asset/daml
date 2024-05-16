// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import com.daml.error.utils.{DecodedCantonError, ErrorDetails}
import com.google.rpc.Status
import io.grpc.Status.Code
import org.scalatest.EitherValues
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class ErrorCodeSpec
    extends AnyFreeSpec
    with Matchers
    with Eventually
    with IntegrationPatience
    with ErrorsAssertions
    with ScalaCheckDrivenPropertyChecks
    with EitherValues {

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

    val maxLength = 512

    "create correct message" in {
      FooErrorCode.toMsg(
        cause = "cause123",
        correlationId = Some("123correlationId"),
        limit = Some(maxLength),
      ) shouldBe "FOO_ERROR_CODE(8,123corre): cause123"
      FooErrorCode.toMsg(
        cause = "cause123",
        correlationId = None,
        limit = Some(maxLength),
      ) shouldBe "FOO_ERROR_CODE(8,0): cause123"
      FooErrorCode.toMsg(
        cause = "x" * maxLength * 2,
        correlationId = Some("123correlationId"),
        limit = Some(maxLength),
      ) shouldBe s"FOO_ERROR_CODE(8,123corre): ${"x" * maxLength}..."
      FooErrorCodeSecuritySensitive.toMsg(
        cause = "cause123",
        correlationId = Some("123correlationId"),
        limit = Some(maxLength),
      ) shouldBe "FOO_ERROR_CODE_SECURITY_SENSITIVE(4,123corre): cause123"
    }

    "create a minimal grpc status and exception" - {

      "when correlation-id and trace-id are not set" in {
        testMinimalGrpcStatus(NoLogging)
      }

      "when only correlation-id is set" in {
        testMinimalGrpcStatus(
          new NoLogging(
            properties = Map.empty,
            correlationId = Some("123correlationId"),
          )
        )
      }

      "when only trace-id is set" in {
        testMinimalGrpcStatus(
          new NoLogging(
            properties = Map.empty,
            // correlationId should be the traceId when not set
            correlationId = Some("123traceId"),
            traceId = Some("123traceId"),
          )
        )
      }

      "when correlation-id and trace-id are set" in {
        testMinimalGrpcStatus(
          new NoLogging(
            properties = Map.empty,
            correlationId = Some("123correlationId"),
            traceId = Some("123traceId"),
          )
        )
      }

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
            "key????" -> "keyWithInvalidCharacters",
          )

        override def definiteAnswerO: Option[Boolean] = Some(false)

        override def throwableO: Option[Throwable] =
          Some(new RuntimeException("runtimeException123"))
      }

      val errorLoggerBig: ContextualizedErrorLogger = new NoLogging(
        correlationId = Some("123correlationId"),
        properties = Map(
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
              "loggingEntryKey" -> "loggingEntryValue",
              "contextKey1" -> "contextValue1",
              "key" -> "keyWithInvalidCharacters",
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
        final case class TestedError() extends FooErrorBig(testedErrorCode)

        val expectedStatus = Status
          .newBuilder()
          .setMessage("FOO_ERROR_CODE(8,123corre): cause123")
          .setCode(testedErrorCode.category.grpcCode.value.value())
          .addAllDetails(details.map(_.toRpcAny).asJava)
          .build()
        val testedError = TestedError()

        assertStatus(
          actual = ErrorCode.asGrpcStatus(testedError)(errorLoggerBig),
          expected = expectedStatus,
        )
        assertError(
          actual = ErrorCode.asGrpcError(testedError)(errorLoggerBig),
          expectedStatusCode = testedErrorCode.category.grpcCode.value,
          expectedMessage = "FOO_ERROR_CODE(8,123corre): cause123",
          expectedDetails = details,
        )
      }

      "security sensitive" in {
        val testedErrorCode = FooErrorCodeSecuritySensitive
        final case class FooError() extends FooErrorBig(testedErrorCode)
        val expectedStatus = Status
          .newBuilder()
          .setMessage(
            BaseError.SecuritySensitiveMessage(Some("123correlationId"))
          )
          .setCode(testedErrorCode.category.grpcCode.value.value())
          .addDetails(requestInfo.toRpcAny)
          .build()
        val testedError = FooError()
        testedError.logWithContext(Map.empty)(errorLoggerBig)

        assertStatus(
          actual = ErrorCode.asGrpcStatus(testedError)(errorLoggerBig),
          expected = expectedStatus,
        )
        assertError(
          actual = ErrorCode.asGrpcError(testedError)(errorLoggerBig),
          expectedStatusCode = testedErrorCode.category.grpcCode.value,
          expectedMessage = BaseError.SecuritySensitiveMessage(Some("123correlationId")),
          expectedDetails = Seq(requestInfo),
        )
      }

    }

    "create a grpc status and exception for input exceeding details size limits" in {
      class FooErrorBig(override val code: ErrorCode) extends BaseError {
        override val cause: String = "cause123"

        override def context: Map[String, String] =
          super.context ++ Map(
            ("y" * ErrorCode.MaxErrorContentBytes) -> ("y" * ErrorCode.MaxErrorContentBytes)
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
              ErrorResource.Party -> ("x" * ErrorCode.MaxErrorContentBytes),
            )

        override def definiteAnswerO: Option[Boolean] = Some(false)
      }
      val errorLoggerOversized: ContextualizedErrorLogger = new NoLogging(
        correlationId = Some("123correlationId"),
        properties = Map(
          "loggingEntryKey" -> "loggingEntryValue",
          "loggingEntryValueTooBig" -> ("x" * ErrorCode.MaxErrorContentBytes),
          ("x" * ErrorCode.MaxErrorContentBytes) -> "loggingEntryKeyTooBig",
        ),
      )
      val requestInfo = ErrorDetails.RequestInfoDetail("123correlationId")
      val retryInfo = ErrorDetails.RetryInfoDetail(123.seconds + 456.milliseconds)

      val testedErrorCode = FooErrorCode
      final case class TestedError() extends FooErrorBig(FooErrorCode)
      val testedError = TestedError()

      val expectedDetails = Seq(
        ErrorDetails
          .ErrorInfoDetail(
            testedErrorCode.id,
            Map(
              "category" -> testedErrorCode.category.asInt.toString,
              "definite_answer" -> "false",
              "loggingEntryKey" -> "loggingEntryValue",
              "loggingEntryValueTooBig" -> ("x" * 849 + "..."),
              "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" -> "loggingEntryKeyTooBig",
              "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" -> ("y" * 809 + "..."),
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
        .setCode(testedErrorCode.category.grpcCode.value.value())
        .addAllDetails(expectedDetails.map(_.toRpcAny).asJava)
        .build()

      assertStatus(
        actual = ErrorCode.asGrpcStatus(testedError)(errorLoggerOversized),
        expected = expectedStatus,
      )
      assertError(
        actual = ErrorCode.asGrpcError(testedError)(errorLoggerOversized),
        expectedStatusCode = testedErrorCode.category.grpcCode.value,
        expectedMessage = expectedMessage,
        expectedDetails = expectedDetails,
      )
    }

    "do not exceed the safe-to-serialize limit" in {
      implicit val generatorDrivenConfig: PropertyCheckConfiguration =
        PropertyCheckConfiguration(minSuccessful = 100)

      // The description gets added to the io.grpc.Status and to the StatusProto as well
      // so we must leave some space for it
      forAll(ErrorGenerator.defaultErrorGen) { err =>
        whenever(
          ErrorCode
            // Ensure we evaluate the test only for errors that must be truncated
            .asGrpcStatus(err, ErrorCode.MaxErrorContentBytes * 10)(err.errorContext)
            .getSerializedSize > ErrorCode.MaxErrorContentBytes
        ) {
          val protoResult = err.asGrpcStatus
          val serializedSize = protoResult.getSerializedSize

          serializedSize should be <= ErrorCode.MaxErrorContentBytes withClue s"for $err"
        }
      }
    }

    "truncate the trace-id if abnormaly large" in {
      val errWithLargeTraceId =
        ErrorGenerator.defaultErrorGen.sample.value.copy(traceId = Some("x" * 1000)).asGrpcError

      DecodedCantonError
        .fromStatusRuntimeException(errWithLargeTraceId)
        .value
        .traceId
        .value shouldBe ("x" * 253 + "...")
    }

    "truncate the correlation-id if abnormaly large" in {
      val errWithLargeTraceId =
        ErrorGenerator.defaultErrorGen.sample.value
          .copy(correlationId = Some("x" * 1000))
          .asGrpcError

      DecodedCantonError
        .fromStatusRuntimeException(errWithLargeTraceId)
        .value
        .correlationId
        .value shouldBe ("x" * 253 + "...")
    }
  }

  def testMinimalGrpcStatus(errorLoggerSmall: ContextualizedErrorLogger): Unit = {
    class FooErrorMinimal(override val code: ErrorCode) extends BaseError {
      override val cause: String = "cause123"
    }
    val testedErrorCode = FooErrorCode
    val id = errorLoggerSmall.correlationId.orElse(errorLoggerSmall.traceId)
    val idTruncated = id.getOrElse("0").take(8)
    final case class TestedError() extends FooErrorMinimal(testedErrorCode)
    val details = Seq(
      ErrorDetails
        .ErrorInfoDetail(
          testedErrorCode.id,
          Map(
            "category" -> testedErrorCode.category.asInt.toString
          ) ++ errorLoggerSmall.traceId.fold(Map.empty[String, String])(tid => Map("tid" -> tid)),
        )
    ) ++ id
      .map(correlationId =>
        ErrorDetails.RequestInfoDetail(
          correlationId = correlationId
        )
      )
      .toList

    val expected = Status
      .newBuilder()
      .setMessage(s"FOO_ERROR_CODE(8,$idTruncated): cause123")
      .setCode(Code.INVALID_ARGUMENT.value())
      .addAllDetails(details.map(_.toRpcAny).asJava)
      .build()
    val testedError = TestedError()

    assertStatus(
      actual = ErrorCode.asGrpcStatus(testedError)(errorLoggerSmall),
      expected = expected,
    )
    assertError(
      actual = ErrorCode.asGrpcError(testedError)(errorLoggerSmall),
      expectedStatusCode = testedErrorCode.category.grpcCode.value,
      expectedMessage = s"FOO_ERROR_CODE(8,$idTruncated): cause123",
      expectedDetails = details,
    )
  }

}
