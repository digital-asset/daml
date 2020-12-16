// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import net.logstash.logback.argument.StructuredArgument
import net.logstash.logback.marker.MapEntriesAppendingMarker
import org.slf4j.Marker

import scala.jdk.CollectionConverters._

object LoggingContext {

  def newLoggingContext[A](kvs: Map[String, String])(f: LoggingContext => A): A =
    f(new LoggingContext(kvs))

  def newLoggingContext[A](f: LoggingContext => A): A =
    newLoggingContext(Map.empty[String, String])(f)

  def newLoggingContext[A](kv: (String, String), kvs: (String, String)*)(
      f: LoggingContext => A,
  ): A =
    newLoggingContext(withEnrichedLoggingContext(kv, kvs: _*)(f)(_))

  /**
    * ## Principles to follow when enriching the logging context
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
    *
    */
  def withEnrichedLoggingContext[A](kvs: Map[String, String])(f: LoggingContext => A)(
      implicit loggingContext: LoggingContext,
  ): A =
    f(loggingContext ++ kvs)

  /**
    * ## Principles to follow when enriching the logging context
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
    *
    */
  def withEnrichedLoggingContext[A](kv: (String, String), kvs: (String, String)*)(
      f: LoggingContext => A,
  )(implicit loggingContext: LoggingContext): A =
    f(loggingContext ++ (kv +: kvs))

  val ForTesting: LoggingContext = new LoggingContext(Map.empty)

}

final class LoggingContext private (ctxMap: Map[String, String]) {

  private lazy val forLogging: Marker with StructuredArgument =
    new MapEntriesAppendingMarker(ctxMap.asJava)

  private[logging] def ifEmpty(doThis: => Unit)(
      ifNot: Marker with StructuredArgument => Unit): Unit =
    if (ctxMap.isEmpty) doThis else ifNot(forLogging)

  private def ++[V](kvs: Iterable[(String, String)]): LoggingContext =
    new LoggingContext(ctxMap ++ kvs)

}
