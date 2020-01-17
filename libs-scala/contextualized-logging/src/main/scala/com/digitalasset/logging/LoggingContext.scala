// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.logging

import net.logstash.logback.argument.StructuredArgument
import net.logstash.logback.marker.MapEntriesAppendingMarker
import org.slf4j.Marker

import scala.collection.JavaConverters.mapAsJavaMapConverter

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object LoggingContext {

  def newLoggingContext[A](f: LoggingContext => A): A =
    f(new LoggingContext(Map.empty))

  def newLoggingContext[A](kv: (String, Any), kvs: (String, Any)*)(f: LoggingContext => A): A =
    f(new LoggingContext((kv +: kvs).toMap))

  def withEnrichedLoggingContext[A](kv: (String, Any), kvs: (String, Any)*)(f: LoggingContext => A)(
      implicit logCtx: LoggingContext): A =
    f((logCtx + kv) ++ kvs)

}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
final class LoggingContext private (ctxMap: Map[String, Any]) {

  private lazy val forLogging: Marker with StructuredArgument =
    new MapEntriesAppendingMarker(ctxMap.asJava)

  private[logging] def ifEmpty(doThis: => Unit)(
      ifNot: Marker with StructuredArgument => Unit): Unit =
    if (ctxMap.isEmpty) doThis else ifNot(forLogging)

  private def +(kv: (String, Any)): LoggingContext = new LoggingContext(ctxMap + kv)
  private def ++(kvs: Seq[(String, Any)]): LoggingContext = new LoggingContext(ctxMap ++ kvs)

}
