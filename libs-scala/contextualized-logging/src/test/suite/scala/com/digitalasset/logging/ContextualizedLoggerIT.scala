// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import java.io.{ByteArrayOutputStream, OutputStream}

import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.filter.ThresholdFilter
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.core.OutputStreamAppender
import ch.qos.logback.core.encoder.Encoder
import com.daml.logging.LoggingContext.{newLoggingContext, withEnrichedLoggingContext}
import net.logstash.logback.encoder.LogstashEncoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

private final case class Entry(
    message: String,
    level: String,
    a: Option[String],
    b: Option[String],
    c: Option[String],
)

final class ContextualizedLoggerIT extends AnyFlatSpec with Matchers {

  behavior of "ContextualizedLogger"

  def testCase(logger: ContextualizedLogger): Unit = {
    newLoggingContext { implicit loggingContext =>
      logger.error("1")
      withEnrichedLoggingContext("a" -> "1") { implicit loggingContext =>
        logger.error("2")
        withEnrichedLoggingContext("b" -> "2") { implicit loggingContext =>
          logger.error("3")
          Await.result(
            withEnrichedLoggingContext("c" -> "3") { implicit loggingContext =>
              Future(logger.error("4"))(concurrent.ExecutionContext.global)
            },
            10.seconds,
          )
          logger.info("3")
        }
        logger.info("2")
      }
      logger.info("1")
    }
  }

  it should "write the expected JSON file" in {

    import io.circe.generic.auto._
    import io.circe.parser._

    val log = ContextualizedLoggerIT.run(new LogstashEncoder)(testCase)

    log map decode[Entry] should contain theSameElementsAs Seq(
      Right(Entry("1", "ERROR", None, None, None)),
      Right(Entry("2", "ERROR", Some("1"), None, None)),
      Right(Entry("3", "ERROR", Some("1"), Some("2"), None)),
      Right(Entry("4", "ERROR", Some("1"), Some("2"), Some("3"))),
      Right(Entry("3", "INFO", Some("1"), Some("2"), None)),
      Right(Entry("2", "INFO", Some("1"), None, None)),
      Right(Entry("1", "INFO", None, None, None)),
    )

  }

  it should "write the expected lines when using a non-structured encoder" in {

    val encoder = new PatternLayoutEncoder()
    encoder.setPattern("%msg%n")

    val log = ContextualizedLoggerIT.run(encoder)(testCase)

    log should contain theSameElementsAs Seq(
      "1",
      "2",
      "3",
      "4",
      "3",
      "2",
      "1",
    )

  }

}

object ContextualizedLoggerIT {

  def run(e: Encoder[ILoggingEvent])(t: ContextualizedLogger => Unit): IndexedSeq[String] = {
    val (logger, output) = setupLogger(e)
    val contextualizedLogger = ContextualizedLogger.createFor(logger)
    t(contextualizedLogger)
    output.close()
    output.toString.split(System.lineSeparator()).toIndexedSeq
  }

  private[this] def setupLogger(encoder: Encoder[ILoggingEvent]): (Logger, OutputStream) = {
    val context = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

    val output = new ByteArrayOutputStream

    encoder.setContext(context)
    encoder.start()

    val filter = new ThresholdFilter
    filter.setLevel("INFO")
    filter.setContext(context)
    filter.start()

    val appender = new OutputStreamAppender[ILoggingEvent]
    appender.setName("LOGGING_TEST")
    appender.setContext(context)
    appender.setOutputStream(output)
    appender.setImmediateFlush(true)
    appender.addFilter(filter)
    appender.setEncoder(encoder)
    appender.start()

    val logger = context.getLogger(classOf[ContextualizedLoggerIT])
    logger.setLevel(Level.INFO)
    logger.addAppender(appender)

    logger -> output
  }

}
