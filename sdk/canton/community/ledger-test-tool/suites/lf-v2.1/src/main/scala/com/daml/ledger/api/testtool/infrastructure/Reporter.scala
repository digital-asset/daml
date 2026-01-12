// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.canton.buildinfo.BuildInfo
import org.scalatest.exceptions.StackDepthException

import java.io.{PrintWriter, StringWriter}
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
      t match {
        case e: StackDepthException =>
          e.position.map(_.lineNumber).orElse(extractLineNumberFromStackTrace(e))
        case _ => extractLineNumberFromStackTrace(t)
      }

    private def extractLineNumberFromStackTrace(t: Throwable): Option[Int] =
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
      reporterPrintln: String => Unit,
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
        reporterPrintln("")
        reporterPrintln(cyan(suite))

        for (LedgerTestSummary(_, name, description, result) <- summaries) {
          reporterPrintln(cyan(s"- [$name] $description ... "))
          result match {
            case Right(Result.Succeeded(duration)) =>
              reporterPrintln(green(s"Success (${duration.toMillis} ms)"))
            case Right(Result.Retired) =>
              reporterPrintln(yellow(s"Skipped (retired test)"))
            case Right(Result.Excluded(reason)) =>
              reporterPrintln(yellow(s"Skipped ($reason)"))
            case Left(Result.TimedOut) => reporterPrintln(red(s"Timeout"))
            case Left(Result.Failed(cause)) =>
              val message =
                extractRelevantLineNumber(cause).fold("Assertion failed") { lineHint =>
                  s"Assertion failed at line $lineHint"
                }
              reporterPrintln(red(message))
              reporterPrintln(red(indented(cause.getMessage)))
              cause match {
                case AssertionErrorWithPreformattedMessage(preformattedMessage, _) =>
                  preformattedMessage.split("\n").map(indented(_)).foreach(reporterPrintln)
                case _ => // ignore
              }
              if (printStackTraces) {
                for (renderedStackTraceLine <- render(cause))
                  reporterPrintln(red(indented(renderedStackTraceLine)))
              }
            case Left(Result.FailedUnexpectedly(cause)) =>
              val prefix =
                s"Unexpected failure (${cause.getClass.getSimpleName})"
              val message =
                extractRelevantLineNumber(cause).fold(prefix) { lineHint =>
                  s"$prefix at line $lineHint"
                }
              reporterPrintln(red(message))
              reporterPrintln(red(indented(cause.getMessage)))
              if (printStackTraces) {
                for (renderedStackTraceLine <- render(cause))
                  reporterPrintln(red(indented(renderedStackTraceLine)))
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

      reporterPrintln("")
      reporterPrintln(blue("#" * 80))
      reporterPrintln(blue("#"))
      reporterPrintln(blue(s"# TEST REPORT, version: ${BuildInfo.version}"))
      reporterPrintln(blue("#"))
      reporterPrintln(blue("#" * 80))
      if (failures.isEmpty && printOnFailuresOnly) {
        reporterPrintln(s"Successful tests: ${successes.size}")
        reporterPrintln(s"Excluded tests: ${excludedTests.size}")
      } else {
        reporterPrintln("")
        reporterPrintln(yellow("### RUN INFORMATION"))
        reporterPrintln("")
        runInfo.foreach { case (label, value) => reporterPrintln(cyan(s"$label = $value")) }
        if (successes.nonEmpty) {
          reporterPrintln("")
          reporterPrintln(green("### SUCCESSES"))
          printReport(successes)
        }

        if (excludedTests.nonEmpty) {
          reporterPrintln("")
          reporterPrintln(yellow("### EXCLUDED TESTS"))
          printReport(excludedTests)
        }

        if (failures.nonEmpty) {
          reporterPrintln("")
          reporterPrintln(red("### FAILURES"))
          printReport(failures)
        }
      }
    }
  }
}
