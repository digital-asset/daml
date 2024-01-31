// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.cliopts

import ch.qos.logback.classic.{Level => LogLevel}

object Logging {

  def reconfigure(clazz: Class[_]): Unit = {
    // Try reconfiguring the library
    import ch.qos.logback.core.joran.spi.JoranException
    import ch.qos.logback.classic.LoggerContext
    import ch.qos.logback.classic.joran.JoranConfigurator
    import java.io.File
    import java.net.URL
    import org.slf4j.LoggerFactory
    def reloadConfig(path: URL): Unit =
      try {
        val context = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
        val configurator = new JoranConfigurator()
        configurator.setContext(context)
        context.reset()
        configurator.doConfigure(path)
      } catch {
        case je: JoranException =>
          // Fallback to System.err.println because the logger won't work in any way anymore.
          System.err.println(
            s"reconfigured failed using url $path: $je"
          )
      }
    System.getProperty("logback.configurationFile") match {
      case null => reloadConfig(clazz.getClassLoader.getResource("logback.xml"))
      case path => reloadConfig(new File(path).toURI().toURL())
    }
  }

  private val KnownLogLevels = Set("ERROR", "WARN", "INFO", "DEBUG", "TRACE")

  sealed trait LogEncoder

  object LogEncoder {
    case object Plain extends LogEncoder
    case object Json extends LogEncoder
  }

  private implicit val scoptLogLevel: scopt.Read[LogLevel] = scopt.Read.reads { level =>
    Either
      .cond(
        KnownLogLevels.contains(level.toUpperCase),
        LogLevel.toLevel(level.toUpperCase),
        s"Unrecognized logging level $level",
      )
      .getOrElse(throw new java.lang.IllegalArgumentException(s"Unknown logging level $level"))
  }

  private implicit val scoptLogEncoder: scopt.Read[LogEncoder] =
    scopt.Read.reads { encoder =>
      encoder.toLowerCase match {
        case "plain" => LogEncoder.Plain
        case "json" => LogEncoder.Json
        case _ =>
          throw new java.lang.IllegalArgumentException(s"Unrecognized logging encoder $encoder")
      }
    }

  /** Parse in the cli option for the logging level.
    */
  def logLevelParse[C](
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

  def logEncoderParse[C](
      parser: scopt.OptionParser[C]
  )(logEncoder: Setter[C, LogEncoder]): Unit = {
    import parser.opt

    opt[LogEncoder]("log-encoder")
      .optional()
      .action {
        case (LogEncoder.Plain, c) => c
        case (encoder, c) => logEncoder(_ => encoder, c)
      }
      .text("Which encoder to use: plain|json")
    ()
  }

  def setUseJsonLogEncoderSystemProp(): Unit = {
    System.setProperty("LOG_FORMAT_JSON", "true")
    ()
  }
}
