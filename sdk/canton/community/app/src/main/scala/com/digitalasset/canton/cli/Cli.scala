// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.cli

import ch.qos.logback.classic.Level
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.discard.Implicits.DiscardOps
import scopt.OptionParser

import java.io.File
import scala.annotation.nowarn

sealed trait LogFileAppender

object LogFileAppender {
  object Rolling extends LogFileAppender

  object Flat extends LogFileAppender

  object Off extends LogFileAppender
}

sealed trait LogEncoder

object LogEncoder {
  object Plain extends LogEncoder

  object Json extends LogEncoder
}

/** CLI Options
  *
  * See the description for each argument in the CLI builder below.
  */
final case class Cli(
    configFiles: Seq[File] = Seq(),
    configMap: Map[String, String] = Map(),
    command: Option[Command] = None,
    noTty: Boolean = false,
    levelRoot: Option[Level] = None,
    levelCanton: Option[Level] = None,
    levelStdout: Level = Level.WARN,
    logFileAppender: LogFileAppender = LogFileAppender.Rolling,
    logFileRollingPattern: Option[String] = None,
    kmsLogFileRollingPattern: Option[String] = None,
    logFileHistory: Option[Int] = None,
    kmsLogFileHistory: Option[Int] = None,
    logTruncate: Boolean = false,
    logFileName: Option[String] = None,
    kmsLogFileName: Option[String] = None,
    logEncoder: LogEncoder = LogEncoder.Plain,
    logLastErrors: Boolean = true,
    logLastErrorsFileName: Option[String] = None,
    logImmediateFlush: Option[Boolean] = None,
    kmsLogImmediateFlush: Option[Boolean] = None,
    bootstrapScriptPath: Option[File] = None,
    manualStart: Boolean = false,
    autoConnectLocal: Boolean = false,
) {

  /** sets the properties our logback.xml is looking for */
  def installLogging(): Unit = {
    setLevel(levelRoot, "LOG_LEVEL_ROOT")

    // The root level can override the canton level if root is configured with a lower level
    val overrideLevelCanton = (for {
      root <- levelRoot
      canton <- levelCanton
    } yield root.levelInt < canton.levelInt).getOrElse(false)
    if (overrideLevelCanton)
      setLevel(levelRoot, "LOG_LEVEL_CANTON")
    else
      setLevel(levelCanton, "LOG_LEVEL_CANTON")

    setLevel(Some(levelStdout), "LOG_LEVEL_STDOUT")

    if (command.isEmpty && !noTty) {
      // Inform logging that this is an interactive console running that needs some additional tweaks
      // for a good logging experience
      System.setProperty("INTERACTIVE_STDOUT", true.toString)
    }

    System.setProperty("LOG_FILE_APPEND", (!logTruncate).toString)

    Seq(
      "LOG_FILE_FLAT",
      "LOG_FILE_ROLLING",
      "LOG_FILE_NAME",
      "KMS_LOG_FILE_NAME",
      "LOG_FILE_ROLLING_PATTERN",
      "KMS_LOG_FILE_ROLLING_PATTERN",
      "LOG_FILE_HISTORY",
      "KMS_LOG_FILE_HISTORY",
      "LOG_LAST_ERRORS",
      "LOG_LAST_ERRORS_FILE_NAME",
      "LOG_FORMAT_JSON",
      "LOG_IMMEDIATE_FLUSH",
      "KMS_LOG_IMMEDIATE_FLUSH",
    ).foreach(System.clearProperty(_).discard[String])
    logFileName.foreach(System.setProperty("LOG_FILE_NAME", _))
    kmsLogFileName.foreach(System.setProperty("KMS_LOG_FILE_NAME", _))
    logLastErrorsFileName.foreach(System.setProperty("LOG_LAST_ERRORS_FILE_NAME", _))
    logFileHistory.foreach(x => System.setProperty("LOG_FILE_HISTORY", x.toString))
    kmsLogFileHistory.foreach(x => System.setProperty("KMS_LOG_FILE_HISTORY", x.toString))
    logFileRollingPattern.foreach(System.setProperty("LOG_FILE_ROLLING_PATTERN", _))
    kmsLogFileRollingPattern.foreach(System.setProperty("KMS_LOG_FILE_ROLLING_PATTERN", _))
    logFileAppender match {
      case LogFileAppender.Rolling =>
        System.setProperty("LOG_FILE_ROLLING", "true").discard
      case LogFileAppender.Flat =>
        System.setProperty("LOG_FILE_FLAT", "true").discard
      case LogFileAppender.Off =>
    }
    if (logLastErrors)
      System.setProperty("LOG_LAST_ERRORS", "true").discard

    logEncoder match {
      case LogEncoder.Plain =>
      case LogEncoder.Json =>
        System.setProperty("LOG_FORMAT_JSON", "true").discard
    }

    logImmediateFlush.foreach(f => System.setProperty("LOG_IMMEDIATE_FLUSH", f.toString))
    kmsLogImmediateFlush.foreach(f => System.setProperty("KMS_LOG_IMMEDIATE_FLUSH", f.toString))
  }

  private def setLevel(levelO: Option[Level], name: String): Unit = {
    val _ = levelO match {
      case Some(level) => System.setProperty(name, level.levelStr)
      case None => System.clearProperty(name)
    }
  }

}

@nowarn(raw"msg=unused value of type .* \(add `: Unit` to discard silently\)")
object Cli {
  // The `additionalVersions` parameter allows the enterprise CLI to output the version of additional,
  // enterprise-only dependencies (see `CantonAppDriver`).
  def parse(args: Array[String], printVersion: => Unit = ()): Option[Cli] =
    parser(printVersion).parse(args, Cli())

  private def parser(printVersion: => Unit): OptionParser[Cli] =
    new scopt.OptionParser[Cli]("canton") {

      private def inColumns(first: String = "", second: String = ""): String =
        f"  $first%-25s$second"

      head("Canton", s"v${BuildInfo.version}")

      help('h', "help").text("Print usage")
      opt[Unit]("version")
        .text("Print versions")
        .action { (_, _) =>
          printVersion.discard
          sys.exit(0)
        }

      opt[Seq[File]]('c', "config")
        .text(
          "Set configuration file(s).\n" +
            inColumns(second = "If several configuration files assign values to the same key,\n") +
            inColumns(second = "the last value is taken.")
        )
        .valueName("<file1>,<file2>,...")
        .unbounded()
        .action((files, cli) => cli.copy(configFiles = cli.configFiles ++ files))

      opt[Map[String, String]]('C', "config key-value's")
        .text(
          "Set configuration key value pairs directly.\n" +
            inColumns(second = "Can be useful for providing simple short config info.")
        )
        .valueName("<key1>=<value1>,<key2>=<value2>")
        .unbounded()
        .action { (map, cli) =>
          cli.copy(configMap =
            map ++ cli.configMap
          ) // the values on the right of the ++ operator are preferred for the same key. thus in case of repeated keys, the first defined is taken.
        }

      opt[File]("bootstrap")
        .text("Set a script to run on startup")
        .valueName("<file>")
        .action((script, cli) => cli.copy(bootstrapScriptPath = Some(script)))

      opt[Unit]("no-tty")
        .text("Do not use a tty")
        .action((_, cli) => cli.copy(noTty = true))

      opt[Unit]("manual-start")
        .text("Don't automatically start the nodes")
        .action((_, cli) => cli.copy(manualStart = true))

      opt[Unit]("auto-connect-local")
        .text("Automatically connect all local participants to all local domains")
        .action((_, cli) => cli.copy(autoConnectLocal = true))

      note(inColumns(first = "-D<property>=<value>", second = "Set a JVM property value"))

      note("\nLogging Options:") // Enforce a newline in the help text

      opt[Unit]('v', "verbose")
        .text("Canton logger level -> DEBUG")
        .action((_, cli) => cli.copy(levelCanton = Some(Level.DEBUG)))

      opt[Unit]("debug")
        .text("Console/stdout level -> INFO, root logger -> DEBUG")
        .action((_, cli) =>
          cli.copy(
            levelRoot = Some(Level.DEBUG),
            levelCanton = Some(Level.DEBUG),
            levelStdout = Level.INFO,
          )
        )

      opt[Unit]("log-truncate")
        .text("Truncate log file on startup.")
        .action((_, cli) => cli.copy(logTruncate = true))

      implicit val levelRead: scopt.Read[Level] = scopt.Read.reads(Level.valueOf)
      opt[Level]("log-level-root")
        .text("Log-level of the root logger")
        .valueName("<LEVEL>")
        .action((level, cli) => cli.copy(levelRoot = Some(level), levelCanton = Some(level)))

      opt[Level]("log-level-canton")
        .text("Log-level of the Canton logger")
        .valueName("<LEVEL>")
        .action((level, cli) => cli.copy(levelCanton = Some(level)))

      opt[Level]("log-level-stdout")
        .text("Log-level of stdout")
        .valueName("<LEVEL>")
        .action((level, cli) => cli.copy(levelStdout = level))

      opt[String]("log-file-appender")
        .text("Type of log file appender")
        .valueName("rolling(default)|flat|off")
        .action((typ, cli) =>
          typ.toLowerCase match {
            case "rolling" => cli.copy(logFileAppender = LogFileAppender.Rolling)
            case "off" => cli.copy(logFileAppender = LogFileAppender.Off)
            case "flat" => cli.copy(logFileAppender = LogFileAppender.Flat)
            case _ =>
              throw new IllegalArgumentException(
                s"Invalid command line argument: unknown log-file-appender $typ"
              )
          }
        )

      opt[String]("log-file-name")
        .text("Name and location of log-file, default is log/canton.log")
        .action((name, cli) => cli.copy(logFileName = Some(name)))

      opt[String]("kms-log-file-name")
        .text("Name and location of KMS log-file, default is log/canton_kms.log")
        .action((name, cli) => cli.copy(kmsLogFileName = Some(name)))

      opt[Int]("log-file-rolling-history")
        .text("Number of history files to keep when using rolling log file appender.")
        .action((history, cli) => cli.copy(logFileHistory = Some(history)))

      opt[Int]("kms-log-file-rolling-history")
        .text("Number of history KMS files to keep when using rolling log file appender.")
        .action((history, cli) => cli.copy(kmsLogFileHistory = Some(history)))

      opt[String]("log-file-rolling-pattern")
        .text("Log file suffix pattern of rolling file appender. Default is 'yyyy-MM-dd'.")
        .action((pattern, cli) => cli.copy(logFileRollingPattern = Some(pattern)))

      opt[String]("kms-log-file-rolling-pattern")
        .text("KMS log file suffix pattern of rolling file appender. Default is 'yyyy-MM-dd'.")
        .action((pattern, cli) => cli.copy(kmsLogFileRollingPattern = Some(pattern)))

      opt[String]("log-encoder")
        .text("Log encoder: plain|json")
        .action {
          case ("json", cli) => cli.copy(logEncoder = LogEncoder.Json)
          case ("plain", cli) => cli.copy(logEncoder = LogEncoder.Plain)
          case (other, _) =>
            throw new IllegalArgumentException(s"Unsupported logging encoder $other")
        }

      opt[Boolean]("log-immediate-flush")
        .text(
          """Determines whether to immediately flush log output to the log file.
            |Enable to avoid an incomplete log file in case of a crash.
            |Disable to reduce the load on the disk caused by logging.""".stripMargin
        )
        .valueName("true(default)|false")
        .action((enabled, cli) => cli.copy(logImmediateFlush = Some(enabled)))

      opt[Boolean]("kms-log-immediate-flush")
        .text(
          """Determines whether to immediately flush KMS log output to the KMS log file.
            |Enable to avoid an incomplete log file in case of a crash.
            |Disable to reduce the load on the disk caused by logging.""".stripMargin
        )
        .valueName("true(default)|false")
        .action((enabled, cli) => cli.copy(kmsLogImmediateFlush = Some(enabled)))

      opt[String]("log-profile")
        .text("Preconfigured logging profiles: (container)")
        .action((profile, cli) =>
          profile.toLowerCase match {
            case "container" =>
              cli.copy(
                logFileAppender = LogFileAppender.Rolling,
                logFileHistory = Some(10),
                logFileRollingPattern = Some("yyyy-MM-dd-HH"),
                levelStdout = Level.DEBUG,
              )
            case _ => throw new IllegalArgumentException(s"Unknown log profile $profile")
          }
        )

      opt[Boolean]("log-last-errors")
        .text("Capture events for logging.last_errors command")
        .action((isEnabled, cli) => cli.copy(logLastErrors = isEnabled))

      note("") // Enforce a newline in the help text

      note("Use the JAVA_OPTS environment variable to set JVM parameters.")

      note("") // Enforce a newline in the help text

      cmd("daemon")
        .text(
          "Start all nodes automatically and run them without having a console (REPL).\n" +
            "Nodes can be controlled through the admin API."
        )
        .action((_, cli) => cli.copy(command = Some(Command.Daemon)))
        .children()

      note("") // Enforce a newline in the help text

      cmd("run")
        .text(
          "Run a console script.\n" +
            "Stop all nodes when the script has terminated."
        )
        .children(
          arg[File]("<file>")
            .text("the script to run")
            .action((script, cli) => cli.copy(command = Some(Command.RunScript(script))))
        )

      note("") // Enforce a newline in the help text

      implicit val readTarget: scopt.Read[Command.Generate.Target] = scopt.Read.reads {
        case "remote-config" => Command.Generate.RemoteConfig
        case x => throw new IllegalArgumentException(s"Unknown target $x")
      }
      cmd("generate")
        .text("Generate configurations")
        .children(
          arg[Command.Generate.Target]("<type>")
            .text("generation target (remote-config)")
            .action((target, cli) => cli.copy(command = Some(Command.Generate(target))))
        )

      checkConfig(cli =>
        if (cli.configFiles.isEmpty && cli.configMap.isEmpty) {
          failure(
            "at least one config has to be defined either as files (-c) or as key-values (-C)"
          )
        } else success
      )

      checkConfig(cli =>
        if (
          cli.autoConnectLocal && cli.command.exists {
            case Command.Daemon => false
            case Command.RunScript(_) => true
            case Command.Generate(_) => true
          }
        ) {
          failure(s"auto-connect-local does not work with run-script or generate")
        } else success
      )

      override def showUsageOnError: Option[Boolean] = Some(true)

    }
}
