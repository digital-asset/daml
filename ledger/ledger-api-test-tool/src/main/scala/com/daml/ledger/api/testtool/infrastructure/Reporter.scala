// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.io.PrintStream

import scala.util.Try

trait Reporter[A] {
  def report(results: Vector[LedgerTestSummary]): A
}

object Reporter {
  object ColorizedPrintStreamReporter {
    private val reset = "\u001b[0m"

    private def red(s: String): String = s"\u001b[31m$s$reset"
    private def green(s: String): String = s"\u001b[32m$s$reset"
    private def yellow(s: String): String = s"\u001b[33m$s$reset"
    private def blue(s: String): String = s"\u001b[34m$s$reset"
    private def cyan(s: String): String = s"\u001b[36m$s$reset"

    private def render(t: Throwable): Seq[String] =
      s"${t.getClass.getName}: ${t.getMessage}" +: t.getStackTrace.map(render)

    private def render(e: StackTraceElement): String =
      s"\tat ${e.getClassName}.${e.getMethodName}(${e.getFileName}:${e.getLineNumber})"

    private def extractRelevantLineNumber(t: Throwable): Option[Int] =
      t.getStackTrace
        .find(
          stackTraceElement =>
            Try(Class.forName(stackTraceElement.getClassName))
              .filter(_ != classOf[LedgerTestSuite])
              .filter(classOf[LedgerTestSuite].isAssignableFrom)
              .isSuccess)
        .map(_.getLineNumber)
  }

  final class ColorizedPrintStreamReporter(s: PrintStream, printStackTraces: Boolean)
      extends Reporter[Unit] {

    import ColorizedPrintStreamReporter._

    private def indented(msg: String, n: Int = 2): String = {
      val indent = " " * n
      msg.lines.map(l => s"$indent$l").mkString("\n")
    }

    private def printReport(results: Vector[LedgerTestSummary]): Unit =
      results.groupBy(_.suite).foreach {
        case (suite, summaries) =>
          s.println()
          s.println(cyan(suite))

          for (LedgerTestSummary(_, test, _, result) <- summaries) {
            s.print(cyan(s"- $test ... "))
            result match {
              case Right(Result.Succeeded(duration)) =>
                s.println(green(s"Success (${duration.toMillis} ms)"))
              case Right(Result.Skipped(reason)) =>
                s.println(yellow(s"Skipped (reason: $reason)"))
              case Left(Result.TimedOut) => s.println(red(s"Timeout"))
              case Left(Result.Failed(cause)) =>
                val message =
                  extractRelevantLineNumber(cause).fold("Assertion failed") { lineHint =>
                    s"Assertion failed at line $lineHint"
                  }
                s.println(red(message))
                s.println(red(indented(cause.getMessage)))
                cause match {
                  case AssertionErrorWithPreformattedMessage(preformattedMessage, _) =>
                    preformattedMessage.split("\n").map(indented(_)).foreach(s.println)
                  case _ => // ignore
                }
                if (printStackTraces) {
                  for (renderedStackTraceLine <- render(cause))
                    s.println(red(indented(renderedStackTraceLine)))
                }
              case Left(Result.FailedUnexpectedly(cause)) =>
                val prefix =
                  s"Unexpected failure (${cause.getClass.getSimpleName})"
                val message =
                  extractRelevantLineNumber(cause).fold(prefix) { lineHint =>
                    s"$prefix at line $lineHint"
                  }
                s.println(red(message))
                s.println(red(indented(cause.getMessage)))
                if (printStackTraces) {
                  for (renderedStackTraceLine <- render(cause))
                    s.println(red(indented(renderedStackTraceLine)))
                }
            }
          }
      }

    override def report(results: Vector[LedgerTestSummary]): Unit = {
      s.println()
      s.println(blue("#" * 80))
      s.println(blue("#"))
      s.println(blue("# TEST REPORT"))
      s.println(blue("#"))
      s.println(blue("#" * 80))

      val (successes, failures) = results.partition(_.result.isRight)

      if (successes.nonEmpty) {
        s.println()
        s.println(green("### SUCCESSES"))
        printReport(successes)
      }

      if (failures.nonEmpty) {
        s.println()
        s.println(red("### FAILURES"))
        printReport(failures)
      }
    }
  }
}
