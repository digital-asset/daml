// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.utils

import com.daml.error.ErrorCategory.BackgroundProcessDegradationWarning
import com.daml.error.definitions.LedgerApiErrors
import com.daml.error.{DamlContextualizedErrorLogger, ErrorClass, ErrorCode}
import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ErrorDetailsSpec extends AnyFlatSpec with Matchers {

  private val errorLogger = DamlContextualizedErrorLogger.forTesting(getClass)

  behavior of classOf[ErrorDetails.type].getName

  it should "correctly match exception to error codes " in {
    val securitySensitive =
      LedgerApiErrors.AuthorizationChecks.Unauthenticated.MissingJwtToken()(errorLogger).asGrpcError
    val notSecuritySensitive = LedgerApiErrors.Admin.UserManagement.UserNotFound
      .Reject(_operation = "operation123", userId = "userId123")(errorLogger)
      .asGrpcError

    ErrorDetails.matches(
      securitySensitive,
      LedgerApiErrors.AuthorizationChecks.Unauthenticated,
    ) shouldBe false
    ErrorDetails.matches(
      notSecuritySensitive,
      LedgerApiErrors.Admin.UserManagement.UserNotFound,
    ) shouldBe true
    ErrorDetails.matches(
      new StatusRuntimeException(Status.ABORTED),
      LedgerApiErrors.Admin.UserManagement.UserNotFound,
    ) shouldBe false
    ErrorDetails.matches(
      new Exception,
      LedgerApiErrors.Admin.UserManagement.UserNotFound,
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
}
