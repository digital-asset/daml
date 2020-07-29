// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import net.logstash.logback.argument.StructuredArgument
import net.logstash.logback.marker.MapEntriesAppendingMarker
import org.slf4j.Marker

import scala.collection.JavaConverters.mapAsJavaMapConverter

object LoggingContext {

  def newLoggingContext[A](kvs: Map[String, String])(f: LoggingContext => A): A =
    f(new LoggingContext(kvs))

  def newLoggingContext[A](f: LoggingContext => A): A =
    newLoggingContext(Map.empty[String, String])(f)

  def newLoggingContext[A](kv: (String, String), kvs: (String, String)*)(
      f: LoggingContext => A,
  ): A =
    newLoggingContext(withEnrichedLoggingContext(kv, kvs: _*)(f)(_))

  def withEnrichedLoggingContext[A](kvs: Map[String, String])(f: LoggingContext => A)(
      implicit loggingContext: LoggingContext,
  ): A =
    f(loggingContext ++ kvs)

  def withEnrichedLoggingContext[A](kv: (String, String), kvs: (String, String)*)(
      f: LoggingContext => A,
  )(implicit loggingContext: LoggingContext): A =
    f(loggingContext ++ (kv +: kvs))

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
