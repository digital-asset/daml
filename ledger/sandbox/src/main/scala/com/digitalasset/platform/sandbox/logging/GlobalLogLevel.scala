// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import ch.qos.logback.classic.Level
import org.slf4j.{Logger, LoggerFactory}

object GlobalLogLevel {
  def set(level: Level): Unit = {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) match {
      case rootLogger: ch.qos.logback.classic.Logger =>
        rootLogger.setLevel(level)
        rootLogger.info(s"Sandbox verbosity changed to $level")
      case rootLogger =>
        rootLogger.warn(s"Sandbox verbosity cannot be set to requested $level")
    }
  }
}
