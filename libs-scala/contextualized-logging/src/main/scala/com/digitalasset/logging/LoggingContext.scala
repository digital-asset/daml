// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.logging

import net.logstash.logback.argument.{StructuredArgument, StructuredArguments}
import net.logstash.logback.marker.MapEntriesAppendingMarker
import org.slf4j.Marker

import scala.annotation.implicitNotFound
import scala.collection.JavaConverters.mapAsJavaMapConverter

object LoggingContext {

  @implicitNotFound(
    "import com.digitalasset.logging.LoggingContext._ and use exclusively native types, strings and arrays as values in the logging context")
  sealed abstract class AllowedValue[V] {
    final def asString(v: V): String = StructuredArguments.toString(v)
  }
  implicit val allowedDouble: AllowedValue[Double] = new AllowedValue[Double] {}
  implicit val allowedFloat: AllowedValue[Float] = new AllowedValue[Float] {}
  implicit val allowedLong: AllowedValue[Long] = new AllowedValue[Long] {}
  implicit val allowedInt: AllowedValue[Int] = new AllowedValue[Int] {}
  implicit val allowedShort: AllowedValue[Short] = new AllowedValue[Short] {}
  implicit val allowedByte: AllowedValue[Byte] = new AllowedValue[Byte] {}
  implicit val allowedChar: AllowedValue[Char] = new AllowedValue[Char] {}
  implicit val allowedBoolean: AllowedValue[Boolean] = new AllowedValue[Boolean] {}
  implicit def allowedString[S <: String]: AllowedValue[S] = new AllowedValue[S] {}
  implicit def allowedArray[A: AllowedValue]: AllowedValue[Array[A]] = new AllowedValue[Array[A]] {}

  def newLoggingContext[A](f: LoggingContext => A): A =
    f(new LoggingContext(Map.empty))

  def newLoggingContext[A, V: AllowedValue](kv: (String, V))(f: LoggingContext => A): A =
    newLoggingContext { implicit logCtx =>
      withEnrichedLoggingContext(kv)(f)
    }

  def withEnrichedLoggingContext[A, V](kv: (String, V))(
      f: LoggingContext => A)(implicit logCtx: LoggingContext, value: AllowedValue[V]): A =
    f(logCtx + (kv._1 -> value.asString(kv._2)))

}

final class LoggingContext private (ctxMap: Map[String, String]) {

  private lazy val forLogging: Marker with StructuredArgument =
    new MapEntriesAppendingMarker(ctxMap.asJava)

  private[logging] def ifEmpty(doThis: => Unit)(
      ifNot: Marker with StructuredArgument => Unit): Unit =
    if (ctxMap.isEmpty) doThis else ifNot(forLogging)

  private def +[V](kv: (String, String)): LoggingContext = new LoggingContext(ctxMap + kv)

}
