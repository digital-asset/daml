// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

/** Abstracts away from the logging tech stack used. */
trait ContextualizedErrorLogger {
  def properties: Map[String, String]
  def correlationId: Option[String]
  def traceId: Option[String]
  def logError(err: BaseError, extra: Map[String, String]): Unit
  def info(message: String): Unit
  def info(message: String, throwable: Throwable): Unit
  def warn(message: String): Unit
  def warn(message: String, throwable: Throwable): Unit
  def error(message: String): Unit
  def error(message: String, throwable: Throwable): Unit
}

object ContextualizedErrorLogger {

  /** Formats the context as a string for logging */
  def formatContextAsString(contextMap: Map[String, String]): String = {
    contextMap
      .filter(_._2.nonEmpty)
      .toSeq
      .sortBy(_._1)
      .map { case (k, v) =>
        s"$k=$v"
      }
      .mkString(", ")
  }

}
