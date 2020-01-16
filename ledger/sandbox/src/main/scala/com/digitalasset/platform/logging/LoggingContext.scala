// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.logging

import net.logstash.logback.argument.StructuredArgument
import net.logstash.logback.marker.MapEntriesAppendingMarker
import org.slf4j.Marker

import scala.collection.JavaConverters.mapAsJavaMapConverter

object LoggingContext {

  def newLoggingContext[A](f: LoggingContext => A): A =
    f(new LoggingContext(Map.empty))

  def newLoggingContext[A](kv: (String, String), kvs: (String, String)*)(
      f: LoggingContext => A): A =
    f(new LoggingContext((kv +: kvs).toMap))

  def withEnrichedLoggingContext[A](kv: (String, String), kvs: (String, String)*)(
      f: LoggingContext => A)(implicit ctx: LoggingContext): A =
    f((ctx + kv) ++ kvs)

}

final class LoggingContext private (ctxMap: Map[String, String]) {

  private lazy val forLogging: Marker with StructuredArgument =
    new MapEntriesAppendingMarker(ctxMap.asJava)

  private[logging] def ifEmpty(doThis: => Unit)(
      ifNot: Marker with StructuredArgument => Unit): Unit =
    if (ctxMap.isEmpty) doThis else ifNot(forLogging)

  private def +(kv: (String, String)): LoggingContext = new LoggingContext(ctxMap + kv)
  private def ++(kvs: Seq[(String, String)]): LoggingContext = new LoggingContext(ctxMap ++ kvs)

}
