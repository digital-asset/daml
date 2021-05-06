// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.cliopts

import ch.qos.logback.classic.{Level => LogLevel}

object Logging {

  private val KnownLogLevels = Set("ERROR", "WARN", "INFO", "DEBUG", "TRACE")

  private implicit val scoptLogLevel: scopt.Read[LogLevel] = scopt.Read.reads { level =>
    Either
      .cond(
        KnownLogLevels.contains(level.toUpperCase),
        LogLevel.toLevel(level.toUpperCase),
        s"Unrecognized logging level $level",
      )
      .getOrElse(throw new java.lang.IllegalArgumentException(s"Unknown logging level $level"))
  }

  /** Parse in the cli option for the logging level.
    */
  def loggingLevelParse[C](
      parser: scopt.OptionParser[C]
  )(logLevel: Setter[C, Option[LogLevel]]): Unit = {
    import parser.opt

    opt[LogLevel]("log-level")
      .optional()
      .action((level, c) => logLevel(_ => Some(level), c))
      .text(
        s"Default logging level to use. Available values are ${KnownLogLevels.mkString(", ")}. Defaults to INFO."
      )
    ()
  }
}
