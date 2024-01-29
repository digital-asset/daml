// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import org.slf4j.LoggerFactory

private[lf] object InternalError {
  private[this] val logger = LoggerFactory.getLogger("com.daml.lf")

  def log(location: String, message: String, cause: Option[Throwable] = None): Unit = {
    logger.error(s"LF internal error in $location: $message")
    cause.foreach(err => logger.error("root cause", err))
  }

  @throws[IllegalArgumentException]
  def illegalArgumentException(location: String, message: String): Nothing = {
    log(location, message)
    throw new IllegalArgumentException(location + ": " + message)
  }

  @throws[IllegalStateException]
  def assertionException(location: String, message: String): Nothing = {
    log(location, message)
    throw new AssertionError(location + ": " + message)
  }

  @throws[RuntimeException]
  def runtimeException(location: String, message: String): Nothing = {
    log(location, message)
    throw new RuntimeException(location + ": " + message)
  }

}

trait InternalError {
  def location: String
  def message: String
  def cause: Option[Throwable]

  InternalError.log(location, message, cause)
}
