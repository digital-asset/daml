// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import ch.qos.logback.classic.Level
import com.daml.error.ErrorCategory.TransientServerFailure
import com.daml.error.utils.testpackage.SeriousError
import com.daml.error.utils.testpackage.subpackage.NotSoSeriousError
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.testing.LogCollector
import com.google.rpc.{ErrorInfo, RequestInfo, RetryInfo}
import io.grpc.protobuf.StatusProto
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class ErrorCodeSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {
  implicit private val testLoggingContext: LoggingContext = LoggingContext.ForTesting
  private val logger = ContextualizedLogger.get(getClass)
  private val errorLoggingContext: Option[String] => DamlErrorCodeLoggingContext = correlationId =>
    new DamlErrorCodeLoggingContext(logger, testLoggingContext, correlationId)

  private val className = classOf[ErrorCode].getSimpleName

  before {
    LogCollector.clear[this.type]
  }

  s"$className.log" should "log the error message with the correct markers" in {
    logSeriousError(
      extra = Map("extra-context-key" -> "extra-context-value")
    )(errorLoggingContext(Some("1234567890")))

    val actualLogs = LogCollector
      .readWithMarkers[this.type, this.type]
      .map { case (level, (errMsg, marker)) =>
        level -> (errMsg -> marker.toString)
      }

    actualLogs.size shouldBe 1
    val (actualLogLevel, (actualLogMessage, actualLogMarker)) = actualLogs.head

    actualLogLevel shouldBe Level.ERROR
    actualLogMessage shouldBe "BLUE_SCREEN(4,12345678): the error argument"
    actualLogMarker should include regex "location=ErrorCodeSpec\\.scala\\:\\d+"
    actualLogMarker should include regex "extra\\-context\\-key=extra\\-context\\-value"
  }

  s"$className.log" should s"truncate the cause size if larger than ${ErrorCode.MaxCauseLogLength}" in {
    val veryLongCause = "o" * (ErrorCode.MaxCauseLogLength * 2)
    logSeriousError(cause = veryLongCause)(errorLoggingContext(None))

    val expectedErrorLog = "BLUE_SCREEN(4,0): " + ("o" * ErrorCode.MaxCauseLogLength + "...")
    val actualLogs = LogCollector.read[this.type, this.type]

    actualLogs shouldBe Seq(Level.ERROR -> expectedErrorLog)
  }

  s"$className.asGrpcErrorFromContext" should "output a GRPC error with correct status, message and metadata" in {
    val error = NotSoSeriousError.Error("some error cause")
    val correlationId = "12345678"

    val actualGrpcError = error.asGrpcErrorFromContext(errorLoggingContext(Some(correlationId)))
    val expectedErrorMessage =
      "UNAVAILABLE: TEST_ROUTINE_FAILURE_PLEASE_IGNORE(1,12345678): Some obscure cause"

    val actualStatus = actualGrpcError.getStatus
    val actualTrailers = actualGrpcError.getTrailers
    val actualRpcStatus = StatusProto.fromStatusAndTrailers(actualStatus, actualTrailers)

    val Seq(rawErrorInfo, rawRetryInfo, rawRequestInfo, rawResourceInfo) =
      actualRpcStatus.getDetailsList.asScala.toSeq

    val actualResourceInfo = rawResourceInfo.unpack(classOf[com.google.rpc.ResourceInfo])
    val actualErrorInfo = rawErrorInfo.unpack(classOf[ErrorInfo])
    val actualRetryInfo = rawRetryInfo.unpack(classOf[RetryInfo])
    val actualRequestInfo = rawRequestInfo.unpack(classOf[RequestInfo])

    actualStatus.getCode shouldBe NotSoSeriousError.category.grpcCode.get
    actualGrpcError.getMessage shouldBe expectedErrorMessage
    actualErrorInfo.getReason shouldBe NotSoSeriousError.id
    actualRetryInfo.getRetryDelay.getSeconds shouldBe TransientServerFailure.retryable.get.duration.toSeconds
    actualRequestInfo.getRequestId shouldBe correlationId
    actualResourceInfo.getResourceType shouldBe error.resources.head._1.asString
    actualResourceInfo.getResourceName shouldBe error.resources.head._2
  }

  private def logSeriousError(
      cause: String = "the error argument",
      extra: Map[String, String] = Map.empty,
  )(implicit errorLoggingContext: ErrorCodeLoggingContext): Unit =
    SeriousError
      .Error(cause)
      .logWithContext(extra)
}
