// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import com.daml.error.ErrorCategory.BackgroundProcessDegradationWarning
import com.daml.error.definitions.CommonErrors
import com.daml.error.{DamlContextualizedErrorLogger, ErrorClass, ErrorCode}
import com.google.protobuf
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class ErrorDetailsSpec extends AnyFlatSpec with Matchers {

  private val errorLogger = DamlContextualizedErrorLogger.forTesting(getClass)

  behavior of classOf[ErrorDetails.type].getName

  it should "correctly match exception to error codes " in {
    val securitySensitive =
      CommonErrors.ServiceInternalError.Generic("some internal failure")(errorLogger).asGrpcError
    val notSecuritySensitive =
      CommonErrors.ServiceNotRunning.Reject("some service")(errorLogger).asGrpcError

    ErrorDetails.matches(
      securitySensitive,
      CommonErrors.ServiceInternalError,
    ) shouldBe false
    ErrorDetails.matches(
      notSecuritySensitive,
      CommonErrors.ServiceNotRunning,
    ) shouldBe true
    ErrorDetails.matches(
      new StatusRuntimeException(Status.ABORTED),
      CommonErrors.ServiceNotRunning,
    ) shouldBe false
    ErrorDetails.matches(
      new Exception,
      CommonErrors.ServiceNotRunning,
    ) shouldBe false

    object NonGrpcErrorCode
        extends ErrorCode(
          id = "NON_GRPC_ERROR_CODE_123",
          BackgroundProcessDegradationWarning,
        )(ErrorClass.root())
    NonGrpcErrorCode.category.grpcCode shouldBe empty
    ErrorDetails.matches(
      new StatusRuntimeException(Status.ABORTED),
      NonGrpcErrorCode,
    ) shouldBe false
  }

  it should "should preserve details when going through grpc Any" in {
    val details = Seq(
      ErrorDetails
        .ErrorInfoDetail(errorCodeId = "errorCodeId1", metadata = Map("a" -> "b", "c" -> "d")),
      ErrorDetails.ResourceInfoDetail(name = "name1", typ = "type1"),
      ErrorDetails.RequestInfoDetail(correlationId = "correlationId1"),
      ErrorDetails.RetryInfoDetail(1.seconds + 2.milliseconds),
    )
    val anys: Seq[protobuf.Any] = details.map(_.toRpcAny)
    ErrorDetails.from(anys) shouldBe details
  }
}
