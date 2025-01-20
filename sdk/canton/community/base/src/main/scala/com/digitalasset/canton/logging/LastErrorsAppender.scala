// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import cats.syntax.functor.*
import cats.syntax.option.*
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.spi.AppenderAttachable
import ch.qos.logback.core.{Appender, AppenderBase}
import com.digitalasset.canton.DiscardOps
import com.github.blemale.scaffeine.Scaffeine

import java.util
import scala.collection.mutable

/** Logback appender that keeps a bounded queue of errors/warnings that have been logged and associated log entries with
  * the same trace-id.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class LastErrorsAppender()
    extends AppenderBase[ILoggingEvent]
    with AppenderAttachable[ILoggingEvent] {

  private val appenders = mutable.ListBuffer[Appender[ILoggingEvent]]()

  /** Treat the last errors file appender separately because we only write there when we encounter an error */
  private var lastErrorsFileAppender: Option[Appender[ILoggingEvent]] = None

  private var maxTraces = 128
  private var maxEvents = 1024
  private var maxErrors = 128
  private var lastErrorsFileAppenderName = ""

  /** An error/warn event with previous events of the same trace-id */
  private case class ErrorWithEvents(error: ILoggingEvent, events: Seq[ILoggingEvent])

  private val eventsCache =
    Scaffeine().maximumSize(maxTraces.toLong).build[String, BoundedQueue[ILoggingEvent]]()
  private val errorsCache =
    Scaffeine().maximumSize(maxErrors.toLong).build[String, ErrorWithEvents]()

  private def isLastErrorsFileAppender(appender: Appender[ILoggingEvent]): Boolean =
    appender.getName == lastErrorsFileAppenderName

  /** Return a map of traceId to error/warning event */
  def lastErrors: Map[String, ILoggingEvent] = errorsCache.asMap().toMap.fmap(_.error)

  /** Returns a trace (sequence of events with the same trace-id) for an error */
  def lastErrorTrace(traceId: String): Option[Seq[ILoggingEvent]] =
    errorsCache.getIfPresent(traceId).map(_.events)

  /** Allows to override the maximum numbers of errors to keep from a logback.xml */
  def setMaxErrors(errors: Int): Unit = {
    maxErrors = errors
  }

  /** Allows to override the maximum events to keep per trace-id from a logback.xml */
  def setMaxEvents(events: Int): Unit = {
    maxEvents = events
  }

  /** Allows to override the maximum traces to keep from a logback.xml */
  def setMaxTraces(traces: Int): Unit = {
    maxTraces = traces
  }

  def setLastErrorsFileAppenderName(name: String): Unit = {
    lastErrorsFileAppenderName = name
  }

  private def getTraceId(event: ILoggingEvent): Option[String] =
    Option(event.getMDCPropertyMap.get(CanLogTraceContext.traceIdMdcKey))

  private def processError(event: ILoggingEvent): Unit = {
    getTraceId(event).foreach { tid =>
      eventsCache.getIfPresent(tid).foreach { relatedEvents =>
        lazy val relatedEventsSeq = relatedEvents.toSeq

        // Update the errors cache with the new related events since the last flush or create a first error entry
        errorsCache
          .asMap()
          .updateWith(tid) {
            case Some(errorWithEvents) =>
              Some(errorWithEvents.copy(events = errorWithEvents.events ++ relatedEventsSeq))
            case None => Some(ErrorWithEvents(event, relatedEventsSeq))
          }
          .discard

        // Flush error with related events to last errors file appender
        lastErrorsFileAppender.foreach { appender =>
          relatedEvents.removeAll().foreach(appender.doAppend)
        }
      }
    }

  }

  private def processEvent(event: ILoggingEvent): Unit = {
    // Always cache the event if it has a trace-id
    getTraceId(event).foreach { tid =>
      eventsCache.asMap().updateWith(tid) { eventsQ =>
        eventsQ.getOrElse(new BoundedQueue[ILoggingEvent](maxEvents)).enqueue(event).some
      }
    }

    // If the event is a warning or error, find the previous related events
    if (event.getLevel.isGreaterOrEqual(Level.WARN))
      processError(event)
  }

  override def append(event: ILoggingEvent): Unit = {
    appenders.foreach(_.doAppend(event))
    processEvent(event)
  }

  override def addAppender(newAppender: Appender[ILoggingEvent]): Unit = {
    if (isLastErrorsFileAppender(newAppender))
      lastErrorsFileAppender = Some(newAppender)
    else
      appenders += newAppender
  }

  override def iteratorForAppenders(): util.Iterator[Appender[ILoggingEvent]] = {
    val it = lastErrorsFileAppender.iterator ++ appenders.iterator
    new util.Iterator[Appender[ILoggingEvent]] {
      override def hasNext: Boolean = it.hasNext
      override def next(): Appender[ILoggingEvent] = it.next()
    }
  }

  override def getAppender(name: String): Appender[ILoggingEvent] = throw new NotImplementedError()

  override def isAttached(appender: Appender[ILoggingEvent]): Boolean =
    appenders.contains(appender) || lastErrorsFileAppender.contains(appender)

  override def detachAndStopAllAppenders(): Unit = {
    appenders.foreach(_.stop())
    appenders.clear()
    lastErrorsFileAppender.foreach(_.stop())
    lastErrorsFileAppender = None
  }

  override def stop(): Unit = {
    super.stop()
    appenders.foreach(_.stop())
    lastErrorsFileAppender.foreach(_.stop())
  }

  override def detachAppender(appender: Appender[ILoggingEvent]): Boolean = {

    if (isLastErrorsFileAppender(appender)) {
      lastErrorsFileAppender = None
      true
    } else {
      val index = appenders.indexOf(appender)
      if (index > -1) {
        appenders.remove(index).discard
      }
      index != -1
    }
  }

  override def detachAppender(name: String): Boolean = throw new NotImplementedError()
}

@SuppressWarnings(Array("org.wartremover.warts.While"))
class BoundedQueue[A](maxQueueSize: Int) extends mutable.Queue[A] {

  private def trim(): Unit = {
    while (size > maxQueueSize) dequeue().discard
  }

  override def addOne(elem: A): BoundedQueue.this.type = {
    val ret = super.addOne(elem)
    trim()
    ret
  }

  override def addAll(elems: IterableOnce[A]): BoundedQueue.this.type = {
    val ret = super.addAll(elems)
    trim()
    ret
  }

}
