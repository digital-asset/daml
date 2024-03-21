// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import org.slf4j.{Logger, Marker}

private[logging] object LeveledLogger {

  final class Trace(logger: Logger) extends LeveledLogger {
    override def isEnabled: Boolean =
      logger.isTraceEnabled()
    override protected def log(msg: String): Unit =
      logger.trace(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.trace(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.trace(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.trace(fmt, arg)
    override protected def log(m: Marker, msg: String): Unit =
      logger.trace(m, msg)
  }

  final class Debug(logger: Logger) extends LeveledLogger {
    override def isEnabled: Boolean =
      logger.isDebugEnabled()
    override protected def log(msg: String): Unit =
      logger.debug(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.debug(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.debug(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.debug(fmt, arg)
    override protected def log(m: Marker, msg: String): Unit =
      logger.debug(m, msg)
  }

  final class Info(logger: Logger) extends LeveledLogger {
    override def isEnabled: Boolean =
      logger.isInfoEnabled()
    override protected def log(msg: String): Unit =
      logger.info(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.info(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.info(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.info(fmt, arg)
    override protected def log(m: Marker, msg: String): Unit =
      logger.info(m, msg)
  }

  final class Warn(logger: Logger) extends LeveledLogger {
    override def isEnabled: Boolean =
      logger.isWarnEnabled()
    override protected def log(msg: String): Unit =
      logger.warn(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.warn(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.warn(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.warn(fmt, arg)
    override protected def log(m: Marker, msg: String): Unit =
      logger.warn(m, msg)
  }

  final class Error(logger: Logger) extends LeveledLogger {
    override def isEnabled: Boolean =
      logger.isErrorEnabled()
    override protected def log(msg: String): Unit =
      logger.error(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.error(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.error(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.error(fmt, arg)
    override protected def log(m: Marker, msg: String): Unit =
      logger.error(m, msg)
  }

}

private[logging] sealed abstract class LeveledLogger {

  def isEnabled: Boolean

  protected def log(msg: String): Unit
  protected def log(msg: String, t: Throwable): Unit
  protected def log(m: Marker, msg: String, t: Throwable): Unit
  protected def log(fmt: String, arg: AnyRef): Unit
  protected def log(m: Marker, msg: String): Unit

  final def apply(msg: => String)(implicit loggingContext: LoggingContext): Unit =
    if (isEnabled)
      loggingContext.ifEmpty(log(msg))(log(_, msg))

  final def apply(msg: => String, t: Throwable)(implicit loggingContext: LoggingContext): Unit =
    if (isEnabled)
      loggingContext.ifEmpty(log(msg, t))(c => log(c, msg, t))

}
