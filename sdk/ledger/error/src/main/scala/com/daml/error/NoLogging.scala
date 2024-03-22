// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error

object NoLogging extends NoLogging(properties = Map.empty, correlationId = None, traceId = None) {}

class NoLogging(
    val properties: Map[String, String],
    val correlationId: Option[String],
    val traceId: Option[String] = None,
) extends ContextualizedErrorLogger {
  override def logError(err: BaseError, extra: Map[String, String]): Unit = ()
  override def info(message: String): Unit = ()
  override def info(message: String, throwable: Throwable): Unit = ()
  override def warn(message: String): Unit = ()
  override def warn(message: String, throwable: Throwable): Unit = ()
  override def error(message: String): Unit = ()
  override def error(message: String, throwable: Throwable): Unit = ()
}
