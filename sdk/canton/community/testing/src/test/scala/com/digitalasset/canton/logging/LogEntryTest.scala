// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.canton.BaseTest
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

class LogEntryTest extends AnyWordSpec with BaseTest {

  val ex = new RuntimeException("test exception")

  "LogEntry" should {
    "pretty print nicely" in {
      val mdc = Map(CanLogTraceContext.traceIdMdcKey -> "mytraceid", "otherKey" -> "otherValue")

      val entry =
        LogEntry(
          Level.WARN,
          "com.digitalasset.canton.MyClass:MyNode",
          "message line 1\nmessage line 2",
          Some(ex),
          mdc,
        )

      entry.toString should startWith(
        """## WARN  c.d.canton.MyClass:MyNode tid:mytraceid - message line 1
          |## 	message line 2
          |## 	MDC: otherKey -> otherValue
          |## 	java.lang.RuntimeException: test exception
          |## 		at com.digitalasset.canton.logging.LogEntryTest.<init>""".stripMargin
      )
    }

    "mention incorrect log level" in {
      val entry = LogEntry(Level.WARN, "", "test")

      val failure = the[TestFailedException] thrownBy entry.errorMessage
      failure.message.value shouldBe
        """Incorrect log level WARN. Expected: ERROR
          |## WARN   - test""".stripMargin
    }

    "mention incorrect log level and logger" in {
      val entry = LogEntry(Level.WARN, "MyLogger", "test")

      val failure = the[TestFailedException] thrownBy entry.commandFailureMessage
      failure.message.value shouldBe
        """Incorrect log level WARN. Expected: ERROR
          |Incorrect logger name MyLogger. Expected one of:
          |  com.digitalasset.canton.integration.CommunityEnvironmentDefinition, com.digitalasset.canton.integration.EnterpriseEnvironmentDefinition
          |## WARN  MyLogger - test""".stripMargin
    }
  }
}
