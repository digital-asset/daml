// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import ch.qos.logback.classic.Level
import com.daml.error.ErrorCategory.TransientServerFailure
import com.daml.error.utils.ErrorDetails
import com.daml.error.utils.testpackage.SeriousError
import com.daml.error.utils.testpackage.subpackage.MildErrorsParent.MildErrors.NotSoSeriousError
import com.daml.platform.testing.LogCollector.ExpectedLogEntry
import com.daml.platform.testing.{LogCollector, LogCollectorAssertions}
import io.grpc.StatusRuntimeException
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ErrorCodeSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfter
    with LogCollectorAssertions
    with ErrorAssertionsWithLogCollectorAssertions {

  private val contextualizedErrorLoggerF =
    (correlationId: Option[String]) =>
      DamlContextualizedErrorLogger.forTesting(getClass, correlationId)

  private val className = classOf[ErrorCode].getSimpleName

  before {
    LogCollector.clear[this.type]
  }

  s"$className.logWithContext" should "log the error message with the correct markers" in {
    val error = SeriousError.Error(
      "the error argument",
      context = Map("extra-context-key" -> "extra-context-value"),
    )

    error.logWithContext()(contextualizedErrorLoggerF(Some("1234567890")))

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

  s"$className.logWithContext" should s"truncate the cause size if larger than ${ErrorCode.MaxCauseLogLength}" in {
    val veryLongCause = "o" * (ErrorCode.MaxCauseLogLength * 2)
    val error =
      SeriousError.Error(veryLongCause, context = Map("extra-context-key" -> "extra-context-value"))

    error.logWithContext()(contextualizedErrorLoggerF(None))

    val expectedErrorLog = "BLUE_SCREEN(4,0): " + ("o" * ErrorCode.MaxCauseLogLength + "...")
    val actualLogs = LogCollector.read[this.type, this.type]
    actualLogs shouldBe Seq(Level.ERROR -> expectedErrorLog)
  }

  s"$className.asGrpcErrorFromContext" should "output a GRPC error with correct status, message and metadata" in {
    val contextMetadata = Map("some key" -> "some value", "another key" -> "another value")
    val error = NotSoSeriousError.Error("some error cause", contextMetadata)
    val correlationId = "12345678"

    val actual: StatusRuntimeException =
      error.asGrpcErrorFromContext(contextualizedErrorLoggerF(Some(correlationId)))

    assertError(
      actual,
      expectedCode = NotSoSeriousError.category.grpcCode.get,
      expectedMessage = "TEST_ROUTINE_FAILURE_PLEASE_IGNORE(1,12345678): Some obscure cause",
      expectedDetails = Seq(
        ErrorDetails.ErrorInfoDetail(
          NotSoSeriousError.id,
          Map("category" -> "1") ++ contextMetadata ++ Map("definite_answer" -> "true"),
        ),
        ErrorDetails.RetryInfoDetail(TransientServerFailure.retryable.get.duration),
        ErrorDetails.RequestInfoDetail(correlationId),
        ErrorDetails.ResourceInfoDetail(error.resources.head._1.asString, error.resources.head._2),
      ),
    )
  }

  s"$className.asGrpcErrorFromContext" should "not propagate security sensitive information in gRPC statuses (with correlation id)" in {
    val error =
      SeriousError.Error("some cause", Map("some sensitive key" -> "some sensitive value"))
    val correlationId = "12345678"
    val contextualizedErrorLogger = contextualizedErrorLoggerF(Some(correlationId))

    val actual: StatusRuntimeException = error.asGrpcErrorFromContext(contextualizedErrorLogger)
    error.logWithContext(Map.empty)(contextualizedErrorLogger)

    assertError[this.type, this.type](
      actual,
      expectedCode = io.grpc.Status.Code.INTERNAL,
      expectedMessage =
        s"An error occurred. Please contact the operator and inquire about the request $correlationId",
      expectedDetails = Seq(ErrorDetails.RequestInfoDetail(correlationId)),
      expectedLogEntry = ExpectedLogEntry(
        Level.ERROR,
        s"BLUE_SCREEN(4,$correlationId): some cause",
        Some(
          "\\{err-context: \"\\{location=ErrorCodeSpec.scala:\\d+, some sensitive key=some sensitive value\\}\"\\}"
        ),
      ),
    )
  }

  s"$className.asGrpcErrorFromContext" should "not propagate security sensitive information in gRPC statuses (without correlation id)" in {
    val error =
      SeriousError.Error("some cause", Map("some sensitive key" -> "some sensitive value"))
    val contextualizedErrorLogger = contextualizedErrorLoggerF(None)

    val actual: StatusRuntimeException = error.asGrpcErrorFromContext(contextualizedErrorLogger)
    error.logWithContext(Map.empty)(contextualizedErrorLogger)

    assertError[this.type, this.type](
      actual,
      expectedCode = io.grpc.Status.Code.INTERNAL,
      expectedMessage =
        s"An error occurred. Please contact the operator and inquire about the request <no-correlation-id>",
      expectedDetails = Seq(),
      expectedLogEntry = ExpectedLogEntry(
        Level.ERROR,
        "BLUE_SCREEN(4,0): some cause",
        Some(
          "\\{err-context: \"\\{location=ErrorCodeSpec.scala:\\d+, some sensitive key=some sensitive value\\}\"\\}"
        ),
      ),
    )
  }

}
