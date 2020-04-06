// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.test

import scala.collection._
import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentHashMap

import com.daml.navigator.test.config.Arguments
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.events._
import org.scalatest.{Args, ConfigMap, Reporter}

import scala.util.Try

object Main extends LazyLogging {
  def main(args: Array[String]): Unit = {
    Arguments.parse(args) match {
      case None =>
        System.exit(1)
      case Some(arguments) =>
        val reporter = new LoggerReporter()
        val status = new BrowserTest(arguments)
          .run(None, Args(reporter = reporter, configMap = new ConfigMap(Map.empty[String, Any])))
        val success = Try(status.succeeds()).getOrElse(false)
        val exitCode = if (success) 0 else 1
        val header =
          """
            | 
            |***************************************************************************
            |
            |     Test Results
            |
            |***************************************************************************
            |
          """.stripMargin
        logger.info(header)
        reporter.results.foreach { kv =>
          logger.info(s"${kv._1}")
          kv._2.foreach(logger.info(_))
        }
        val results = reporter.results.toList
        val allTests = results.size
        val failedTests = results.count(kv => kv._2.isDefined)
        val footer =
          s"""
            | 
            |***************************************************************************
            |
            |     All tests: $allTests; tests failed: $failedTests
            |
            |***************************************************************************
            |
          """.stripMargin
        logger.info(footer)
        System.exit(exitCode)
    }
  }
}

class LoggerReporter extends Reporter {

  // Test statuses with optional errors
  val results: concurrent.Map[String, Option[String]] =
    new ConcurrentHashMap[String, Option[String]]().asScala

  override def apply(e: Event): Unit = {
    e match {
      case t: TestSucceeded =>
        results.put(s"  Test succeeded: ${t.testName}", None)
        ()
      case t: TestFailed =>
        results.put(
          s"  Test failed: ${t.testName}",
          t.throwable.map(_.getMessage).map(e => s"      error: $e")
        )
        ()

      case _ => ()
    }

  }

}
