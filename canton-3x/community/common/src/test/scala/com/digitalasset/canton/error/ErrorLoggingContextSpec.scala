// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import com.daml.error.{BaseError, ErrorCategory, ErrorClass, ErrorCode}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.error.testpackage.SeriousError
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.freespec.AnyFreeSpec

class ErrorLoggingContextSpec extends AnyFreeSpec with BaseTest {

  object FooErrorCodeSecuritySensitive
      extends ErrorCode(
        "FOO_ERROR_CODE_SECURITY_SENSITIVE",
        ErrorCategory.SystemInternalAssumptionViolated,
      )(ErrorClass.root())

  classOf[ErrorLoggingContext].getSimpleName - {

    "log information pertaining to a security sensitive error" - {
      class FooErrorBig(override val code: ErrorCode) extends BaseError {
        override val cause: String = "cause123"

        override def context: Map[String, String] =
          super.context ++ Map(
            "contextKey1" -> "contextValue1",
            "key????" -> "keyWithInvalidCharacters",
          )

        override def throwableO: Option[Throwable] =
          Some(new RuntimeException("runtimeException123"))
      }

      val traceContext: TraceContext = nonEmptyTraceContext1

      val errorLoggerBig = ErrorLoggingContext.forClass(
        loggerFactory,
        getClass,
        Map(
          "propertyKey" -> "propertyValue"
        ),
        traceContext,
      )
      val correlationId =
        errorLoggerBig.correlationId.valueOrFail("correlationId was not set")
      val correlationIdTruncated = correlationId.take(8)

      val testedError = new FooErrorBig(FooErrorCodeSecuritySensitive)

      loggerFactory.assertSingleErrorLogEntry(
        within = testedError.logWithContext()(errorLoggerBig),
        expectedMsg = s"FOO_ERROR_CODE_SECURITY_SENSITIVE(4,$correlationIdTruncated): cause123",
        expectedMDC = Map(
          "propertyKey" -> "propertyValue",
          "err-context" -> s"{contextKey1=contextValue1, key????=keyWithInvalidCharacters, location=$this.scala:<line-number>}",
        ),
        expectedThrowable = Some(
          new java.lang.RuntimeException("runtimeException123")
        ),
      )

    }

    "log the error message with the correct mdc" in {
      val error = SeriousError.Error(
        "the error argument",
        context = Map("extra-context-key" -> "extra-context-value"),
      )

      val traceContext: TraceContext = nonEmptyTraceContext1

      val errorLogger = ErrorLoggingContext.forClass(
        loggerFactory,
        getClass,
        Map(),
        traceContext,
      )

      val correlationId = errorLogger.correlationId.valueOrFail("correlationId was not set")
      val correlationIdTruncated = correlationId.take(8)

      loggerFactory.assertLogs(
        within = error.logWithContext()(errorLogger),
        assertions = logEntry => {
          logEntry.errorMessage shouldBe s"BLUE_SCREEN(4,$correlationIdTruncated): the error argument"

          val mdc = logEntry.mdc.map { case (k, v) =>
            (k, v.replaceAll("\\.scala:\\d+", ".scala:<line-number>"))
          }
          mdc should contain(
            "err-context" -> s"{extra-context-key=extra-context-value, location=$this.scala:<line-number>}"
          )
        },
      )
    }

    s"truncate the cause size if larger than ${ErrorCode.MaxCauseLogLength}" in {
      val veryLongCause = "o" * (ErrorCode.MaxCauseLogLength * 2)
      val error =
        SeriousError.Error(
          veryLongCause,
          context = Map("extra-context-key" -> "extra-context-value"),
        )

      val contextualizedErrorLogger = ErrorLoggingContext.fromTracedLogger(logger)

      val expectedErrorLog = "BLUE_SCREEN(4,0): " + ("o" * ErrorCode.MaxCauseLogLength + "...")
      loggerFactory.assertLogs(
        within = error.logWithContext()(contextualizedErrorLogger),
        assertions = _.errorMessage shouldBe expectedErrorLog,
      )
    }
  }

}
