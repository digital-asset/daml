// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.canton.buildinfo.BuildInfo

import java.io.{PrintStream, PrintWriter, StringWriter}
import scala.util.Try

trait Reporter[A] {
  def report(
      results: Vector[LedgerTestSummary],
      skippedTests: Vector[LedgerTestSummary],
      runInfo: Seq[(String, String)],
  ): A
}

object Reporter {
  object ColorizedPrintStreamReporter {
    private val reset = "\u001b[0m"

    private def red(s: String): String = s"\u001b[31m$s$reset"

    private def green(s: String): String = s"\u001b[32m$s$reset"

    private def yellow(s: String): String = s"\u001b[33m$s$reset"

    private def blue(s: String): String = s"\u001b[34m$s$reset"

    private def cyan(s: String): String = s"\u001b[36m$s$reset"

    private def render(t: Throwable): Iterator[String] = {
      val stringWriter = new StringWriter
      val writer = new PrintWriter(stringWriter)
      t.printStackTrace(writer)
      stringWriter.toString.linesIterator
    }

    private def extractRelevantLineNumber(t: Throwable): Option[Int] =
      t.getStackTrace
        .find(stackTraceElement =>
          Try(Class.forName(stackTraceElement.getClassName))
            .filter(_ != classOf[LedgerTestSuite])
            .filter(classOf[LedgerTestSuite].isAssignableFrom)
            .isSuccess
        )
        .map(_.getLineNumber)
  }

  final class ColorizedPrintStreamReporter(
      s: PrintStream,
      printStackTraces: Boolean,
      printOnFailuresOnly: Boolean = false,
  ) extends Reporter[Unit] {

    import ColorizedPrintStreamReporter.*

    private def indented(msg: String, n: Int = 2): String = {
      val indent = " " * n
      if (msg != null) msg.linesIterator.map(l => s"$indent$l").mkString("\n")
      else ""
    }

    private def printReport(results: Vector[LedgerTestSummary]): Unit =
      results.groupBy(_.suite).foreach { case (suite, summaries) =>
        s.println()
        s.println(cyan(suite))

        for (LedgerTestSummary(_, name, description, result) <- summaries) {
          s.print(cyan(s"- [$name] $description ... "))
          result match {
            case Right(Result.Succeeded(duration)) =>
              s.println(green(s"Success (${duration.toMillis} ms)"))
            case Right(Result.Retired) =>
              s.println(yellow(s"Skipped (retired test)"))
            case Right(Result.Excluded(reason)) =>
              s.println(yellow(s"Skipped ($reason)"))
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

    override def report(
        results: Vector[LedgerTestSummary],
        excludedTests: Vector[LedgerTestSummary],
        runInfo: Seq[(String, String)],
    ): Unit = {
      val (successes, failures) = results.partition(_.result.isRight)

      s.println()
      s.println(blue("#" * 80))
      s.println(blue("#"))
      s.println(blue(s"# TEST REPORT, version: ${BuildInfo.version}"))
      s.println(blue("#"))
      s.println(blue("#" * 80))
      if (failures.isEmpty && printOnFailuresOnly) {
        s.println(s"Successful tests: ${successes.size}")
        s.println(s"Excluded tests: ${excludedTests.size}")
      } else {
        s.println()
        s.println(yellow("### RUN INFORMATION"))
        s.println()
        runInfo.foreach { case (label, value) => s.println(cyan(s"$label = $value")) }
        if (successes.nonEmpty) {
          s.println()
          s.println(green("### SUCCESSES"))
          printReport(successes)
        }

        if (excludedTests.nonEmpty) {
          s.println()
          s.println(yellow("### EXCLUDED TESTS"))
          printReport(excludedTests)
        }

        if (failures.nonEmpty) {
          s.println()
          s.println(red("### FAILURES"))
          printReport(failures)
        }
      }
    }
  }
}
