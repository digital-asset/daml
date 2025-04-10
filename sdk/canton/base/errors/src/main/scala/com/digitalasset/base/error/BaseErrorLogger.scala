// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.base.error

trait BaseErrorLogger {
  def logError(err: BaseError, extra: Map[String, String]): Unit
  def correlationId: Option[String]
  def traceId: Option[String]
  def properties: Map[String, String]

  // Error construction warnings/errors
  def warn(message: String): Unit
  def error(message: String, throwable: Throwable): Unit
}
object NoBaseLogging
    extends NoBaseLogging(properties = Map.empty, correlationId = None, traceId = None) {}

class NoBaseLogging(
    val properties: Map[String, String],
    val correlationId: Option[String],
    val traceId: Option[String] = None,
) extends BaseErrorLogger {
  override def logError(err: BaseError, extra: Map[String, String]): Unit = ()
  override def warn(message: String): Unit = ()
  override def error(message: String, throwable: Throwable): Unit = ()
}
