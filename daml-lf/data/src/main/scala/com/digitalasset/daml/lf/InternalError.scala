// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import org.slf4j.LoggerFactory

private[lf] object InternalError {
  private[this] val logger = LoggerFactory.getLogger("com.daml.lf")

  def log(location: String, message: String): Unit =
    logger.error(s"LF internal error in $location: $message")

  @throws[IllegalArgumentException]
  def illegalArgumentException(location: String, message: String): Nothing = {
    log(location, message)
    throw new IllegalArgumentException(message)
  }

  @throws[RuntimeException]
  def runtimeException(location: String, message: String): Nothing = {
    log(location, message)
    throw new RuntimeException(message)
  }
}

trait InternalError {
  def location: String
  def message: String

  InternalError.log(location, message)
}
