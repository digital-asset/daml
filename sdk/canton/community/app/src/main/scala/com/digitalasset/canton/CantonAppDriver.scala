// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import better.files.File
import cats.syntax.either.*
import ch.qos.logback.classic.{Logger, LoggerContext}
import ch.qos.logback.core.status.{ErrorStatus, Status, StatusListener, WarnStatus}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.cli.{Cli, Command, LogFileAppender}
import com.digitalasset.canton.config.ConfigErrors.CantonConfigError
import com.digitalasset.canton.config.{CantonConfig, ConfigErrors, Generate}
import com.digitalasset.canton.environment.{Environment, EnvironmentFactory}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.version.ReleaseVersion
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicReference
import scala.util.control.NonFatal

/** The Canton main application.
  *
  * Starts a set of domains and participant nodes.
  */
abstract class CantonAppDriver[E <: Environment] extends App with NamedLogging with NoTracing {

  protected def environmentFactory: EnvironmentFactory[E]

  protected def withManualStart(config: E#Config): E#Config

  protected def additionalVersions: Map[String, String] = Map.empty

  protected def printVersion(): Unit = {
    (Map(
      "Canton" -> BuildInfo.version,
      "Daml Libraries" -> BuildInfo.damlLibrariesVersion,
      "Stable Canton protocol versions" -> BuildInfo.stableProtocolVersions.toString(),
      "Preview Canton protocol versions" -> BuildInfo.previewProtocolVersions.toString(),
    ) ++ additionalVersions) foreach { case (name, version) =>
      Console.out.println(s"$name: $version")
    }
  }

  // BE CAREFUL: Set the environment variables before you touch anything related to
  // logback as otherwise, the logback configuration will be read without these
  // properties being considered
  private val cliOptions = Cli.parse(args, printVersion()).getOrElse(sys.exit(1))
  cliOptions.installLogging()

  // Fail, if the log configuration cannot be read.
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  private val logbackStatusManager = loggerContext.getStatusManager
  private val killingStatusListener: StatusListener = {
    case status @ (_: WarnStatus | _: ErrorStatus) =>
      Console.err.println(s"Unable to load log configuration.\n$status")
      Console.err.flush()
      sys.exit(-1)
    case _: Status => // ignore
  }
  logbackStatusManager.add(killingStatusListener)

  // Use the root logger as named logger to avoid a prefix "CantonApp" in log files.
  override val loggerFactory: NamedLoggerFactory = NamedLoggerFactory.root

  // Adjust root and canton loggers which works even if a custom logback.xml is defined
  Seq(
    (cliOptions.levelCanton, "com.digitalasset"),
    (cliOptions.levelCanton, "com.daml"),
    (cliOptions.levelRoot, org.slf4j.Logger.ROOT_LOGGER_NAME),
  )
    .foreach {
      case (Some(level), loggerName) =>
        @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
        val root: Logger = LoggerFactory.getLogger(loggerName).asInstanceOf[Logger]
        root.setLevel(level)
      case (None, _) =>
    }

  logger.info(s"Starting Canton version ${ReleaseVersion.current}")
  if (cliOptions.logTruncate) {
    cliOptions.logFileAppender match {
      case LogFileAppender.Rolling =>
        logger.warn(
          "Ignoring log file truncation request, as it only works with flat log files, but here we use rolling log files."
        )
      case LogFileAppender.Flat =>
      case LogFileAppender.Off =>
    }
  }

  // Now that at least one line has been logged, deregister the killingStatusManager so that
  // Canton does not die on a warning status.
  logbackStatusManager.remove(killingStatusListener)

  private val environmentRef: AtomicReference[Option[E]] = new AtomicReference(None)
  sys.runtime.addShutdownHook(new Thread(() => {
    try {
      logger.info("Shutting down...")
      environmentRef.get().foreach(_.close())
      logger.info("Shutdown complete.")
    } catch {
      case NonFatal(exception) =>
        logger.error("Failed to shut down successfully.", exception)
    } finally {
      LoggerFactory.getILoggerFactory match {
        case logbackLoggerContext: LoggerContext =>
          logger.info("Shutting down logger. Bye bye.")
          logbackLoggerContext.stop()
        case _ =>
          logger.warn(
            "Logback is not bound via slf4j. Cannot shut down logger, this could result in lost log-messages."
          )
      }
    }
  }))
  logger.debug("Registered shutdown-hook.")

  val cantonConfig: E#Config = {
    val mergedUserConfigsE = NonEmpty.from(cliOptions.configFiles) match {
      case None if cliOptions.configMap.isEmpty =>
        Left(ConfigErrors.NoConfigFiles.Error())
      case None => Right(ConfigFactory.empty())
      case Some(neConfigFiles) => CantonConfig.parseAndMergeJustCLIConfigs(neConfigFiles)
    }
    val mergedUserConfigs =
      mergedUserConfigsE.valueOr { _ =>
        sys.exit(1)
      }

    val configFromMap = {
      import scala.jdk.CollectionConverters.*
      ConfigFactory.parseMap(cliOptions.configMap.asJava)
    }
    val finalConfig = CantonConfig.mergeConfigs(mergedUserConfigs, Seq(configFromMap))

    val loadedConfig = loadConfig(finalConfig) match {
      case Left(_) =>
        if (cliOptions.configFiles.sizeCompare(1) > 0)
          writeConfigToTmpFile(mergedUserConfigs)
        sys.exit(1)
      case Right(loaded) =>
        if (cliOptions.manualStart) withManualStart(loaded)
        else loaded
    }
    if (loadedConfig.monitoring.logging.logConfigOnStartup) {
      // we have two ways to log the config. both have their pro and cons.
      // full means we include default values. in such a case, it's hard to figure
      // out what really the config settings are.
      // the other method just uses the loaded `Config` object that doesn't have default
      // values, but therefore needs a separate way to handle the rendering
      logger.info(
        "Starting up with resolved config:\n" +
          (if (loadedConfig.monitoring.logging.logConfigWithDefaults)
             loadedConfig.dumpString
           else
             CantonConfig.renderForLoggingOnStartup(finalConfig))
      )
    }
    loadedConfig
  }

  private def writeConfigToTmpFile(mergedUserConfigs: Config) = {
    val tmp = File.newTemporaryFile("canton-config-error-", ".conf")
    logger.error(
      s"An error occurred after parsing a config file that was obtained by merging multiple config " +
        s"files. The resulting merged-together config file, for which the error occurred, was written to '$tmp'."
    )
    tmp
      .write(
        mergedUserConfigs
          .root()
          .render(CantonConfig.defaultConfigRenderer)
      )
      .discard
  }

  // verify that run script and bootstrap script aren't mixed
  if (cliOptions.bootstrapScriptPath.isDefined) {
    cliOptions.command match {
      case Some(Command.RunScript(_)) =>
        logger.error("--bootstrap script and run script are mutually exclusive")
        sys.exit(1)
      case Some(Command.Generate(_)) =>
        logger.error("--bootstrap script and generate are mutually exclusive")
        sys.exit(1)
      case _ =>
    }
  }

  private lazy val bootstrapScript: Option[CantonScript] =
    cliOptions.bootstrapScriptPath
      .map(CantonScriptFromFile)

  val runner: Runner[E] = cliOptions.command match {
    case Some(Command.Daemon) => new ServerRunner(bootstrapScript, loggerFactory)
    case Some(Command.RunScript(script)) => ConsoleScriptRunner(script, loggerFactory)
    case Some(Command.Generate(target)) =>
      Generate.process(target, cantonConfig)
      sys.exit(0)
    case _ =>
      new ConsoleInteractiveRunner(cliOptions.noTty, bootstrapScript, loggerFactory)
  }

  val environment = environmentFactory.create(cantonConfig, loggerFactory)
  environmentRef.set(Some(environment)) // registering for graceful shutdown
  environment.startAndReconnect(cliOptions.autoConnectLocal) match {
    case Right(()) =>
    case Left(_) => sys.exit(1)
  }

  runner.run(environment)

  def loadConfig(config: Config): Either[CantonConfigError, E#Config]

}
