// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.base.error.{BaseError, BaseErrorLogger}

/** Abstracts away from the logging tech stack used. */
trait ContextualizedErrorLogger extends BaseErrorLogger {
  def properties: Map[String, String]
  def correlationId: Option[String]
  def traceId: Option[String]
  def debug(message: String): Unit
  def debug(message: String, throwable: Throwable): Unit
  def info(message: String): Unit
  def info(message: String, throwable: Throwable): Unit
  def warn(message: String): Unit
  def warn(message: String, throwable: Throwable): Unit
  def error(message: String): Unit
  def error(message: String, throwable: Throwable): Unit
  def withContext[A](context: Map[String, String])(body: => A): A
}

object ContextualizedErrorLogger {

  /** Formats the context as a string for logging */
  def formatContextAsString(contextMap: Map[String, String]): String =
    contextMap
      .filter(_._2.nonEmpty)
      .toSeq
      .sortBy(_._1)
      .map { case (k, v) =>
        s"$k=$v"
      }
      .mkString(", ")

}

class NoLogging(
    val properties: Map[String, String],
    val correlationId: Option[String],
    val traceId: Option[String] = None,
) extends ContextualizedErrorLogger {
  override def logError(err: BaseError, extra: Map[String, String]): Unit = ()
  override def debug(message: String): Unit = ()
  override def debug(message: String, throwable: Throwable): Unit = ()
  override def info(message: String): Unit = ()
  override def info(message: String, throwable: Throwable): Unit = ()
  override def warn(message: String): Unit = ()
  override def warn(message: String, throwable: Throwable): Unit = ()
  override def error(message: String): Unit = ()
  override def error(message: String, throwable: Throwable): Unit = ()
  override def withContext[A](context: Map[String, String])(body: => A): A = body
}

object NoLogging extends NoLogging(properties = Map.empty, correlationId = None, traceId = None) {}
