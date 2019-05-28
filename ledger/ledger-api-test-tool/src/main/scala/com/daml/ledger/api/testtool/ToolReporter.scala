// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool

import org.apache.commons.lang3.exception.ExceptionUtils
import org.scalatest.{events => e}
import org.scalatest.events.Event
import org.scalatest.Reporter

/**
  * Ledger API Test Tool CLI reporter. Implements scalatest's Reporter interface and prints out colorized reports to the
  * stdout. Supports very limited set of scalatest events.
  */
class ToolReporter extends Reporter {

  final val ansiReset = "\u001b[0m"
  final val ansiBlue = "\u001b[34m"
  final val ansiGreen = "\u001b[32m"
  final val ansiCyan = "\u001b[36m"
  final val ansiYellow = "\u001b[33m"
  final val ansiRed = "\u001b[31m"

  private def repeatChar(char: Char, n: Int) = char.toString * n

  private var depth = 0

  private def indented(s: String, extra: Integer = 0, prefix: Char = '-') = {
    s.split("\n").map(indentedSingle(_, extra, prefix)).mkString("\n")
  }

  private def indentedSingle(s: String, extra: Integer = 0, prefix: Char = '-') = {
    val d = depth + extra
    val l = s"${prefix} "
    d match {
      case 0 => s
      case _ =>
        repeatChar(' ', d * 2) + l + s
    }
  }

  override def apply(event: Event): Unit = {
    event match {
      case e.TestStarting(
          ordinal,
          suiteName,
          suiteId,
          suiteClassName,
          testName,
          testText,
          formatter,
          location,
          rerunner,
          payload,
          threadName,
          timeStamp) =>
        print(indented(ansiBlue + testText + "... "))

      case e.TestSucceeded(
          ordinal,
          suiteName,
          suiteId,
          suiteClassName,
          testName,
          testText,
          recordedEvents,
          duration,
          formatter,
          location,
          rerunner,
          payload,
          threadName,
          timeStamp) =>
        println(ansiGreen + "✓")

      case e.TestCanceled(
          ordinal,
          message,
          suiteName,
          suiteId,
          suiteClassName,
          testName,
          testText,
          recordedEvents,
          throwable,
          duration,
          formatter,
          location,
          rerunner,
          payload,
          threadName,
          timeStamp) =>
        println(ansiRed + "cancelled.")

      case e.TestFailed(
          ordinal,
          message,
          suiteName,
          suiteId,
          suiteClassName,
          testName,
          testText,
          recordedEvents,
          throwable,
          duration,
          formatter,
          location,
          rerunner,
          payload,
          threadName,
          timeStamp) =>
        println(ansiRed + "✗" + ansiReset)
        throwable match {
          case None =>
            println(indented(ansiRed + s"Exception details missing!", 1, ' '))
          case Some(e) =>
            println(indented(s"Failure details:", 1, ' '))
            val st = ExceptionUtils.getStackTrace(e)
            println(indented(st, 2, '|'))
        }

      case e.ScopeOpened(
          ordinal,
          message,
          nameInfo,
          formatter,
          location,
          payload,
          threadName,
          timeStamp) =>
        println(indented(ansiYellow + message))
        depth += 1
        ()

      case e.ScopeClosed(
          ordinal,
          message,
          nameInfo,
          formatter,
          location,
          payload,
          threadName,
          timeStamp) =>
        depth -= 1
        ()

      case _ =>
        println(
          s"BUG: Unknown reported event: $event. Report the issue to Digital Asset at https://docs.daml.com/support/support.html")
    }
    print(ansiReset)
  }

}
