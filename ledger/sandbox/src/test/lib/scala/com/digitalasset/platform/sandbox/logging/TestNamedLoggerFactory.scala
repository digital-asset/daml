// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import ch.qos.logback.{classic => logback}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.sandbox.logging.TestNamedLoggerFactory._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

class TestNamedLoggerFactory(override val name: String) extends NamedLoggerFactory {

  private val loggers = mutable.Map[String, (Logger, ListAppender[ILoggingEvent])]()
  private val cleanupOperations = mutable.Buffer[() => Unit]()

  override def append(subName: String): NamedLoggerFactory =
    if (name.isEmpty)
      new TestNamedLoggerFactory(subName)
    else
      new TestNamedLoggerFactory(s"$name/$subName")

  override protected def getLogger(fullName: String): Logger =
    loggers
      .getOrElseUpdate(
        fullName, {
          val logger = LoggerFactory.getLogger(fullName).asInstanceOf[logback.Logger]
          val additive = logger.isAdditive
          logger.setAdditive(false)
          val appender = new ListAppender[ILoggingEvent]
          val normalAppenders = logger.iteratorForAppenders().asScala.toVector
          normalAppenders.foreach(logger.detachAppender)
          logger.addAppender(appender)
          appender.start()
          cleanupOperations += (() => {
            logger.detachAppender(appender)
            normalAppenders.foreach(logger.addAppender)
            logger.setAdditive(additive)
          })
          (logger, appender)
        }
      )
      ._1

  def logs[C](klass: Class[C]): Seq[LogEvent] = {
    val fullName = Array(klass.getName, name)
      .filterNot(_.isEmpty)
      .mkString(":")
    logs(fullName)
  }

  def logs(fullName: String): Seq[LogEvent] =
    loggers
      .get(fullName)
      .map(_._2.events)
      .getOrElse(Seq.empty)
      .map(event => event.getLevel -> event.getMessage)

  def cleanup(): Unit = {
    for (operation <- cleanupOperations)
      operation()
    cleanupOperations.clear()
    loggers.clear()
  }
}

object TestNamedLoggerFactory {

  type LogEvent = (Level, String)

  def apply(name: String): TestNamedLoggerFactory = new TestNamedLoggerFactory(name)

  def apply(cls: Class[_]): TestNamedLoggerFactory = apply(cls.getSimpleName)

  def forParticipant(name: String): TestNamedLoggerFactory =
    root.forParticipant(name).asInstanceOf[TestNamedLoggerFactory]

  def root: NamedLoggerFactory = TestNamedLoggerFactory("")

  private class ListAppender[E] extends AppenderBase[E] {
    private val eventsBuffer = mutable.Buffer[E]()

    override def append(eventObject: E): Unit = {
      eventsBuffer += eventObject
    }

    def events: Seq[E] =
      eventsBuffer
  }
}
