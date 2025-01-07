// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.typesafe.scalalogging.Logger
import org.scalactic.source
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers.*
import org.slf4j.event.Level

import java.util.concurrent.TimeUnit
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*

/** Test logger that just writes the events into a queue for inspection
  */
@SuppressWarnings(Array("org.wartremover.warts.Null"))
class NamedEventCapturingLogger(
    val name: String,
    val properties: ListMap[String, String] = ListMap.empty,
    outputLogger: Option[Logger] = None,
    skip: LogEntry => Boolean = _ => false,
) extends NamedLoggerFactory {

  val eventQueue: java.util.concurrent.BlockingQueue[LogEntry] =
    new java.util.concurrent.LinkedBlockingQueue[LogEntry]()

  private def pollEventQueue(ts: Option[Long]): LogEntry = {
    val event = ts match {
      case None => eventQueue.poll()
      case Some(millis) => eventQueue.poll(millis, TimeUnit.MILLISECONDS)
    }
    if (event != null) {
      outputLogger.foreach(
        _.debug(s"Captured ${event.loggerName} ${event.level} ${event.message}")
      )
    }
    event
  }

  val eventSeq: Seq[LogEntry] = eventQueue.asScala.toSeq

  private val logger = new BufferingLogger(eventQueue, name, skip(_))

  override def appendUnnamedKey(key: String, value: String): NamedLoggerFactory = this
  override def append(key: String, value: String): NamedLoggerFactory = this

  override private[logging] def getLogger(fullName: String): Logger = Logger(logger)

  def tryToPollMessage(
      expectedMessage: String,
      expectedLevel: Level,
      expectedThrowable: Throwable = null,
  ): Boolean = {
    val event = eventQueue.peek()
    if (event != null && eventMatches(event, expectedMessage, expectedLevel, expectedThrowable)) {
      pollEventQueue(None)
      true
    } else {
      false
    }
  }

  def eventMatches(
      event: LogEntry,
      expectedMessage: String,
      expectedLevel: Level,
      expectedThrowable: Throwable = null,
  ): Boolean =
    event.message == expectedMessage && event.level == expectedLevel && event.throwable == Option(
      expectedThrowable
    )

  def assertNextMessageIs(
      expectedMessage: String,
      expectedLevel: Level,
      expectedThrowable: Throwable = null,
      timeoutMillis: Long = 2000,
  )(implicit pos: source.Position): Assertion =
    assertNextMessage(_ shouldBe expectedMessage, expectedLevel, expectedThrowable, timeoutMillis)

  def assertNextMessage(
      messageAssertion: String => Assertion,
      expectedLevel: Level,
      expectedThrowable: Throwable = null,
      timeoutMillis: Long = 2000,
  )(implicit pos: source.Position): Assertion =
    assertNextEvent(
      { event =>
        withClue("Unexpected log message: ") {
          messageAssertion(event.message)
        }
        withClue("Unexpected log level: ") {
          event.level shouldBe expectedLevel
        }
        withClue("Unexpected throwable: ") {
          event.throwable shouldBe Option(expectedThrowable)
        }
      },
      timeoutMillis,
    )

  def assertNextEvent(assertion: LogEntry => Assertion, timeoutMillis: Long = 2000)(implicit
      pos: source.Position
  ): Assertion =
    Option(pollEventQueue(Some(timeoutMillis))) match {
      case None => fail("Missing log event.")
      case Some(event) => assertion(event)
    }

  def assertNoMoreEvents(timeoutMillis: Long = 0)(implicit pos: source.Position): Assertion =
    Option(eventQueue.poll(timeoutMillis, TimeUnit.MILLISECONDS)) match {
      case None => succeed
      case Some(event) => fail(s"Unexpected log event: $event")
    }
}
