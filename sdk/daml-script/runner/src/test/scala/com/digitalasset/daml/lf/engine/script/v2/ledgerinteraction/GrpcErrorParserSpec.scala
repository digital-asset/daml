// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.v2.ledgerinteraction

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class GrpcErrorParserSpec extends AsyncFreeSpec with Matchers {
  "GrpcErrorParser" - {
    "parse external-call preparation failures" in {
      errorDetails(
        "INTERPRETATION_EXTERNAL_CALL_PREPARATION_FAILED",
        Seq(
          ExternalCallExtensionId -> "test-extension",
          ExternalCallFunctionId -> "test-function",
          ExceptionText -> "bad config",
        ),
      ) shouldBe SubmitError.ExternalCallError.PreparationFailed(
        "test-extension",
        "test-function",
        "bad config",
      )
    }

    "parse external-call execution failures" in {
      errorDetails(
        "INTERPRETATION_EXTERNAL_CALL_EXECUTION_FAILED",
        Seq(
          ExternalCallExtensionId -> "test-extension",
          ExternalCallFunctionId -> "test-function",
          ExceptionText -> "call failed",
        ),
      ) shouldBe SubmitError.ExternalCallError.ExecutionCallFailed(
        "test-extension",
        "test-function",
        "call failed",
      )
    }

    "parse external-call invalid output failures" in {
      errorDetails(
        "INTERPRETATION_EXTERNAL_CALL_INVALID_OUTPUT",
        Seq(
          ExternalCallExtensionId -> "test-extension",
          ExternalCallFunctionId -> "test-function",
          ExceptionText -> "bad output",
        ),
      ) shouldBe SubmitError.ExternalCallError.ExecutionInvalidOutput(
        "test-extension",
        "test-function",
        "bad output",
      )
    }

    "parse external-call resources independent of order and extra resources" in {
      errorDetails(
        "INTERPRETATION_EXTERNAL_CALL_PREPARATION_FAILED",
        Seq(
          ExceptionText -> "bad config",
          TemplateId -> "ignored:module:Template",
          ExternalCallFunctionId -> "test-function",
          ExternalCallExtensionId -> "test-extension",
        ),
      ) shouldBe SubmitError.ExternalCallError.PreparationFailed(
        "test-extension",
        "test-function",
        "bad config",
      )
    }

    "degrade malformed external-call resources to truncated errors" in {
      val expected = SubmitError.TruncatedError(
        "PreparationFailed",
        "INTERPRETATION_EXTERNAL_CALL_PREPARATION_FAILED(9,XXXXXXXX): external call test",
      )

      Seq(
        Seq(
          ExternalCallExtensionId -> "test-extension",
          ExceptionText -> "bad config",
        ),
        Seq(
          ExternalCallExtensionId -> "test-extension",
          ExternalCallExtensionId -> "duplicate-extension",
          ExternalCallFunctionId -> "test-function",
          ExceptionText -> "bad config",
        ),
      ).map(resources =>
        errorDetails(
          "INTERPRETATION_EXTERNAL_CALL_PREPARATION_FAILED",
          resources,
        )
      ) shouldBe Seq(expected, expected)
    }
  }

  private def errorDetails(
      errorCodeId: String,
      resources: Seq[(String, String)],
  ): SubmitError =
    GrpcErrorParser.convertErrorDetails(
      errorCodeId,
      s"$errorCodeId(9,XXXXXXXX): external call test",
      resources,
    )

  private val ExternalCallExtensionId = "EXTERNAL_CALL_EXTENSION_ID"
  private val ExternalCallFunctionId = "EXTERNAL_CALL_FUNCTION_ID"
  private val ExceptionText = "EXCEPTION_TEXT"
  private val TemplateId = "TEMPLATE_ID"
}
