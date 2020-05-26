// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.logging

import org.slf4j.{Logger, Marker}

private[logging] object LeveledLogger {

  final class Trace(logger: Logger) extends LeveledLogger {
    override protected def isEnabled: Boolean =
      logger.isTraceEnabled()
    override protected def log(msg: String): Unit =
      logger.trace(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.trace(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.trace(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.trace(fmt, arg)
  }

  final class Debug(logger: Logger) extends LeveledLogger {
    override protected def isEnabled: Boolean =
      logger.isDebugEnabled()
    override protected def log(msg: String): Unit =
      logger.debug(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.debug(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.debug(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.debug(fmt, arg)
  }

  final class Info(logger: Logger) extends LeveledLogger {
    override protected def isEnabled: Boolean =
      logger.isInfoEnabled()
    override protected def log(msg: String): Unit =
      logger.info(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.info(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.info(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.info(fmt, arg)
  }

  final class Warn(logger: Logger) extends LeveledLogger {
    override protected def isEnabled: Boolean =
      logger.isWarnEnabled()
    override protected def log(msg: String): Unit =
      logger.warn(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.warn(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.warn(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.warn(fmt, arg)
  }

  final class Error(logger: Logger) extends LeveledLogger {
    override protected def isEnabled: Boolean =
      logger.isErrorEnabled()
    override protected def log(msg: String): Unit =
      logger.error(msg)
    override protected def log(msg: String, t: Throwable): Unit =
      logger.error(msg, t)
    override protected def log(m: Marker, msg: String, t: Throwable): Unit =
      logger.error(m, msg, t)
    override protected def log(fmt: String, arg: AnyRef): Unit =
      logger.error(fmt, arg)
  }

}

private[logging] sealed abstract class LeveledLogger {

  protected def isEnabled: Boolean

  protected def log(msg: String): Unit
  protected def log(msg: String, t: Throwable): Unit
  protected def log(m: Marker, msg: String, t: Throwable): Unit
  protected def log(fmt: String, arg: AnyRef): Unit

  final def apply(msg: => String)(implicit logCtx: LoggingContext): Unit =
    if (isEnabled)
      logCtx.ifEmpty(log(msg))(log(s"$msg (context: {})", _))

  final def apply(msg: => String, t: Throwable)(implicit logCtx: LoggingContext): Unit =
    if (isEnabled)
      logCtx.ifEmpty(log(msg, t))(c => log(c, s"$msg (context: $c)", t))

}
