// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import cats.syntax.functorFilter.*
import ch.qos.logback.classic
import ch.qos.logback.classic.spi.{ILoggingEvent, IThrowableProxy, LoggerContextVO}
import ch.qos.logback.core.spi.AppenderAttachable
import ch.qos.logback.core.{Appender, AppenderBase}
import com.digitalasset.canton.DiscardOps
import org.slf4j.Marker
import org.slf4j.event.KeyValuePair

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.mutable.ListBuffer

/** Rewrite object used for logback.xml */
final class Rewrite {

  val logger = new AtomicReference[String]("")
  val level = new AtomicReference[classic.Level](classic.Level.ERROR)
  // All contains strings must match the logger message for the rewrite
  val contains = new AtomicReference[Seq[String]](Seq())
  val exceptionMessage = new AtomicReference[String]("")
  val isTesting = new AtomicBoolean(false)

  def setMaxLevel(levelStr: String): Unit = level.set(classic.Level.toLevel(levelStr))
  def setLogger(loggerStr: String): Unit = logger.set(loggerStr)
  def setContains(containsStr: String): Unit = {
    val _ = contains.updateAndGet(_ :+ containsStr)
  }
  def setExceptionMessage(exceptionMessageStr: String): Unit =
    exceptionMessage.set(exceptionMessageStr)
  def setTesting(isTesting: Boolean): Unit = this.isTesting.set(isTesting)
}

/** Rewriting log appender
  *
  * Allows to rewrite log levels of external appenders. A logback implementation of
  * http://logging.apache.org/log4j/1.2/apidocs/org/apache/log4j/rewrite/RewriteAppender.html
  *
  * If testing is set to true, we will include the rewrite rules that are marked as testing only.
  *
  * Usage:
  * <appender name="REWRITE_LOG_LEVEL" class="com.digitalasset.canton.logging.RewritingAppender">
  *   <testing>true</testing>
  *   <appender-ref ref="CANTON_TEST_LOG" />
  *   <rewrite class="com.digitalasset.canton.logging.Rewrite">
  *     <logger>io.grpc.netty.NettyServerStream</logger>
  *     <maxLevel>INFO</maxLevel>
  *   </rewrite>
  *   <rewrite class="com.digitalasset.canton.logging.Rewrite">
  *     <logger>com.digitalasset.canton.participant.ParticipantNodeInit:test/ParticipantRestartTest/participant/participant1</logger>
  *     <contains>Unclean shutdown due to cancellation in</contains>
  *     <exceptionMessage>test exception</exceptionMessage>
  *     <maxLevel>INFO</maxLevel>
  *     <testing>true</testing>
  *   </rewrite>
  * </appender>
  */
class RewritingAppender()
    extends AppenderBase[ILoggingEvent]
    with AppenderAttachable[ILoggingEvent] {

  private case class MyRule(
      maxLevel: classic.Level,
      contains: Seq[String],
      exceptionMessage: String,
  )

  private val appenders = ListBuffer[Appender[ILoggingEvent]]()
  private val rules = new AtomicReference[Map[String, Seq[MyRule]]](Map())
  private val testing = new AtomicBoolean(false)

  private class ReLog(event: ILoggingEvent, level: classic.Level) extends ILoggingEvent {
    override def getThreadName: String = event.getThreadName
    override def getLevel: classic.Level = level
    override def getMessage: String = event.getMessage
    override def getArgumentArray: Array[AnyRef] = event.getArgumentArray
    override def getFormattedMessage: String = event.getFormattedMessage
    override def getLoggerName: String = event.getLoggerName
    override def getLoggerContextVO: LoggerContextVO = event.getLoggerContextVO
    override def getThrowableProxy: IThrowableProxy = event.getThrowableProxy
    override def getCallerData: Array[StackTraceElement] = event.getCallerData
    override def hasCallerData: Boolean = event.hasCallerData
    override def getMDCPropertyMap: util.Map[String, String] = event.getMDCPropertyMap
    override def getMdc: util.Map[String, String] = throw new NotImplementedError(
      "deprecated method"
    )
    override def getTimeStamp: Long = event.getTimeStamp
    override def prepareForDeferredProcessing(): Unit = event.prepareForDeferredProcessing()

    override def getKeyValuePairs: util.List[KeyValuePair] = event.getKeyValuePairs

    override def getMarkerList: util.List[Marker] = event.getMarkerList

    override def getNanoseconds: Int = event.getNanoseconds

    override def getSequenceNumber: Long = event.getSequenceNumber
  }

  def setTesting(isTesting: Boolean): Unit = {
    this.testing.set(isTesting)
  }

  def setRewrite(rule: Rewrite): Unit = {
    if (!rule.isTesting.get() || testing.get) {
      val _ = rules.updateAndGet { map =>
        val loggerName = rule.logger.get()
        val current = map.getOrElse(loggerName, Seq())
        val myRule = MyRule(rule.level.get(), rule.contains.get(), rule.exceptionMessage.get())
        map + (loggerName -> (current :+ myRule))
      }
    }
  }

  override def append(event: ILoggingEvent): Unit = {

    def evalRule(rule: MyRule): Option[classic.Level] =
      for {
        _ <- if (rule.maxLevel.isGreaterOrEqual(event.getLevel)) None else Some(())
        _ <-
          if (rule.contains.forall(r => event.getFormattedMessage.contains(r))) Some(())
          else None
        _ <-
          if (
            rule.exceptionMessage.isEmpty ||
            Option(event.getThrowableProxy).exists(_.getMessage.contains(rule.exceptionMessage))
          ) {
            Some(())
          } else None
      } yield rule.maxLevel
    val forward = (for {
      loggerRules <- getRules(event.getLoggerName)
      maxLevel <- loggerRules.mapFilter(evalRule).headOption
    } yield new ReLog(event, maxLevel)).getOrElse(event)

    appenders.foreach(_.doAppend(forward))
  }

  private def getRules(loggerName: String): Option[Seq[MyRule]] = {
    def stripAfter(chr: Char): List[String] = {
      val idx = loggerName.indexOf(chr.toInt)
      if (idx == -1) List() else List(loggerName.take(idx))
    }
    // optionally strip the context information when trying to match the logger
    val tmp = rules.get()
    (List(loggerName) ++ stripAfter(':') ++ stripAfter('/')).map(tmp.get).collectFirst {
      case Some(rules) => rules
    }
  }

  override def addAppender(newAppender: Appender[ILoggingEvent]): Unit =
    appenders += newAppender

  override def iteratorForAppenders(): util.Iterator[Appender[ILoggingEvent]] = {
    val it = appenders.iterator
    new util.Iterator[Appender[ILoggingEvent]] {
      override def hasNext: Boolean = it.hasNext
      override def next(): Appender[ILoggingEvent] = it.next()
    }
  }

  override def getAppender(name: String): Appender[ILoggingEvent] = throw new NotImplementedError()

  override def isAttached(appender: Appender[ILoggingEvent]): Boolean = appenders.contains(appender)

  override def detachAndStopAllAppenders(): Unit = {
    appenders.foreach(_.stop())
    appenders.clear()
  }

  override def stop(): Unit = {
    super.stop()
    appenders.foreach(_.stop())
  }

  override def detachAppender(appender: Appender[ILoggingEvent]): Boolean = {
    val index = appenders.indexOf(appender)
    if (index > -1) {
      appenders.remove(index).discard
    }
    index != -1
  }

  override def detachAppender(name: String): Boolean = throw new NotImplementedError()
}
