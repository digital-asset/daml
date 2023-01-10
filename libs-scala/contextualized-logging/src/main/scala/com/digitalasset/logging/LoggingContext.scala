// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import com.daml.logging.entries._
import net.logstash.logback.argument.StructuredArgument
import org.slf4j.Marker

object LoggingContext {

  val ForTesting: LoggingContext = new LoggingContext(LoggingEntries.empty)

  val empty: LoggingContext = new LoggingContext(LoggingEntries.empty)

  def apply(entry: LoggingEntry, entries: LoggingEntry*): LoggingContext =
    new LoggingContext(LoggingEntries(entry +: entries: _*))

  def enriched(entry: LoggingEntry, entries: LoggingEntry*)(implicit
      loggingContext: LoggingContext
  ): LoggingContext = {
    loggingContext ++ LoggingEntries(entry +: entries: _*)
  }

  private[logging] def newLoggingContext[A](entries: LoggingEntries)(f: LoggingContext => A): A =
    f(new LoggingContext(entries))

  def newLoggingContext[A](f: LoggingContext => A): A =
    newLoggingContext(LoggingEntries.empty)(f)

  def newLoggingContextWith[A](entry: LoggingEntry, entries: LoggingEntry*)(
      f: LoggingContext => A
  ): A =
    newLoggingContext(withEnrichedLoggingContext(entry, entries: _*)(f)(_))

  /** ## Principles to follow when enriching the logging context
    *
    * ### Don't add values coming from a scope outside of the current method
    *
    * If a method receives a value as a parameter, it should trust that,
    * if it was relevant, the caller already added this value to the context.
    * Add values to the context as upstream as possible in the call chain.
    * This ensures to not add duplicates, possibly using slightly different
    * names to track the same value. The context was implemented to ensure
    * that values did not have to be passed down the entire call stack to
    * be logged at relevant points.
    *
    * ### Don't dump string representations of complex objects
    *
    * The purpose of the context is to be consumed by structured logging
    * frameworks. Dumping the string representation of an object, like a
    * Scala case class instance, means embedding some form of string
    * formatting in another (likely to be JSON). This can be difficult
    * to manage and parse, so stick to simple values (strings, numbers,
    * dates, etc.).
    */
  def withEnrichedLoggingContext[A](entry: LoggingEntry, entries: LoggingEntry*)(
      f: LoggingContext => A
  )(implicit loggingContext: LoggingContext): A =
    f(loggingContext ++ LoggingEntries(entry +: entries: _*))

  /** ## Principles to follow when enriching the logging context
    *
    * ### Don't add values coming from a scope outside of the current method
    *
    * If a method receives a value as a parameter, it should trust that,
    * if it was relevant, the caller already added this value to the context.
    * Add values to the context as upstream as possible in the call chain.
    * This ensures to not add duplicates, possibly using slightly different
    * names to track the same value. The context was implemented to ensure
    * that values did not have to be passed down the entire call stack to
    * be logged at relevant points.
    *
    * ### Don't dump string representations of complex objects
    *
    * The purpose of the context is to be consumed by structured logging
    * frameworks. Dumping the string representation of an object, like a
    * Scala case class instance, means embedding some form of string
    * formatting in another (likely to be JSON). This can be difficult
    * to manage and parse, so stick to simple values (strings, numbers,
    * dates, etc.).
    */
  def withEnrichedLoggingContextFrom[A](entries: LoggingEntries)(
      f: LoggingContext => A
  )(implicit loggingContext: LoggingContext): A =
    f(loggingContext ++ entries)
}

final class LoggingContext private (val entries: LoggingEntries) {

  private lazy val forLogging: Marker with StructuredArgument =
    new LoggingMarker(entries.contents)

  private[logging] def ifEmpty(doThis: => Unit)(
      ifNot: Marker with StructuredArgument => Unit
  ): Unit =
    if (entries.isEmpty) doThis else ifNot(forLogging)

  private def ++[V](other: LoggingEntries): LoggingContext =
    new LoggingContext(entries ++ other)
}
