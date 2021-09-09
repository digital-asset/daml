// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

import ch.qos.logback.classic.Level
import com.daml.error.utils.testpackage.SeriousError
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.testing.LogCollector
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ErrorCodeSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {
  implicit private val testLoggingContext: LoggingContext = LoggingContext.ForTesting
  private val logger = ContextualizedLogger.get(getClass)

  private val className = classOf[ErrorCode].getSimpleName

  s"$className.log" should "log the error message" in {
    SeriousError.Error("the error argument").logWithContext(logger)

    val actualLogs = LogCollector.read[this.type, this.type]
    actualLogs shouldBe Seq(Level.ERROR -> "the error argument")
  }

  s"$className.log" should s"truncate the cause size if larger than ${ErrorCode.MaxCauseLogLength}" in {
    val veryLongCause = "o" * (ErrorCode.MaxCauseLogLength * 2)
    SeriousError.Error(veryLongCause).logWithContext(logger)
    val expectedErrorLog = "o" * ErrorCode.MaxCauseLogLength + "..."

    val actualLogs = LogCollector.read[this.type, this.type]
    actualLogs shouldBe Seq(Level.ERROR -> expectedErrorLog)
  }

  before {
    LogCollector.clear[this.type]
  }
}
