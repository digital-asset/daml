// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.io.PrintStream

import scala.util.Try

trait Reporter[A] extends ((Vector[LedgerTestSummary]) => A)

object Reporter {

  object ColorizedPrintStreamReporter {

    private val reset = "\u001b[0m"

    private def red(s: String): String = s"\u001b[31m$s$reset"
    private def green(s: String): String = s"\u001b[32m$s$reset"
    private def yellow(s: String): String = s"\u001b[33m$s$reset"
    private def blue(s: String): String = s"\u001b[34m$s$reset"
    private def cyan(s: String): String = s"\u001b[36m$s$reset"

    private def render(configuration: LedgerSessionConfiguration): String =
      s"address: ${configuration.host}:${configuration.port}, ssl: ${configuration.ssl.isDefined}"

    private def render(t: Throwable): Seq[String] =
      s"${t.getClass.getName}: ${t.getMessage}" +: t.getStackTrace.map(render)

    private def render(e: StackTraceElement): String =
      s"\tat ${e.getClassName}.${e.getMethodName}(${e.getFileName}:${e.getLineNumber})"

    private def extractRelevantLineNumberFromAssertionError(
        assertionError: AssertionError): Option[Int] =
      assertionError.getStackTrace
        .find(
          stackTraceElement =>
            Try(Class.forName(stackTraceElement.getClassName))
              .filter(classOf[LedgerTestSuite].isAssignableFrom)
              .isSuccess)
        .map(_.getLineNumber)

  }

  final class ColorizedPrintStreamReporter(s: PrintStream, printStackTraces: Boolean)
      extends Reporter[Unit] {

    import ColorizedPrintStreamReporter._

    override def apply(results: Vector[LedgerTestSummary]): Unit = {

      s.println()
      s.println(blue("#" * 80))
      s.println(blue("#"))
      s.println(blue("# TEST REPORT"))
      s.println(blue("#"))
      s.println(blue("#" * 80))

      for (LedgerTestSummary(suite, test, configuration, result) <- results.sortBy(_.suite)) {
        s.println()
        s.println(cyan(suite))
        s.println(cyan(test))
        s.println(cyan(render(configuration)))
        result match {
          case Result.Succeeded => s.println(green(s"The test was successful."))
          case Result.TimedOut => s.println(red(s"The test TIMED OUT!"))
          case Result.Skipped(reason) =>
            s.println(yellow(s"The test was skipped (reason: $reason)"))
          case Result.Failed(cause) =>
            val message =
              extractRelevantLineNumberFromAssertionError(cause).fold(
                s"The test FAILED: ${cause.getMessage}") { lineHint =>
                s"The test FAILED at line $lineHint: ${cause.getMessage}"
              }
            s.println(red(message))
            if (printStackTraces) {
              for (renderedStackTraceLine <- render(cause)) s.println(red(renderedStackTraceLine))
            }
          case Result.FailedUnexpectedly(cause) =>
            s.println(red(s"The test FAILED DUE TO AN UNEXPECTED EXCEPTION: ${cause.getMessage}"))
            if (printStackTraces) {
              for (renderedStackTraceLine <- render(cause)) s.println(red(renderedStackTraceLine))
            }
        }
      }
    }
  }

}
