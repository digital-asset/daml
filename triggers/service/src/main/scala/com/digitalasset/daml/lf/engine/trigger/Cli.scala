// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.http.scaladsl.model.Uri
import ch.qos.logback.classic.Level
import com.daml.lf.speedy.Compiler
import com.daml.platform.services.time.TimeProviderType

import java.io.File
import java.nio.file.{Path, Paths}
import java.time.Duration
import com.daml.cliopts

import scala.concurrent.duration.FiniteDuration
import com.daml.auth.middleware.api.{Client => AuthClient}
import com.daml.dbutils.{DBConfig, JdbcConfig}
import com.daml.lf.engine.trigger.TriggerRunnerConfig.DefaultTriggerRunnerConfig
import com.daml.metrics.api.reporters.MetricsReporter
import com.typesafe.scalalogging.StrictLogging
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures

import scala.concurrent.duration
import scalaz.syntax.std.option._

private[trigger] final case class Cli(
    configFile: Option[File],
    // For convenience, we allow passing DARs on startup
    // as opposed to uploading them dynamically.
    darPaths: List[Path],
    address: String,
    httpPort: Int,
    ledgerHost: String,
    ledgerPort: Int,
    authInternalUri: Option[Uri],
    authExternalUri: Option[Uri],
    authBothUri: Option[Uri],
    authRedirectToLogin: AuthClient.RedirectToLogin,
    authCallbackUri: Option[Uri],
    maxInboundMessageSize: Int,
    minRestartInterval: FiniteDuration,
    maxRestartInterval: FiniteDuration,
    maxAuthCallbacks: Int,
    authCallbackTimeout: FiniteDuration,
    maxHttpEntityUploadSize: Long,
    httpEntityUploadTimeout: FiniteDuration,
    timeProviderType: TimeProviderType,
    commandTtl: Duration,
    init: Boolean,
    jdbcConfig: Option[JdbcConfig],
    portFile: Option[Path],
    allowExistingSchema: Boolean,
    compilerConfig: Compiler.Config,
    triggerConfig: TriggerRunnerConfig,
    rootLoggingLevel: Option[Level],
    logEncoder: LogEncoder,
    metricsReporter: Option[MetricsReporter] = None,
    metricsReportingInterval: FiniteDuration = FiniteDuration(10, duration.SECONDS),
) extends StrictLogging {

  def loadFromConfigFile: Option[Either[ConfigReaderFailures, TriggerServiceAppConf]] =
    configFile.map(cf => ConfigSource.file(cf).load[TriggerServiceAppConf])

  def loadFromCliArgs: ServiceConfig = {
    ServiceConfig(
      darPaths = darPaths,
      address = address,
      httpPort = httpPort,
      ledgerHost = ledgerHost,
      ledgerPort = ledgerPort,
      authInternalUri = authInternalUri,
      authExternalUri = authExternalUri,
      authBothUri = authBothUri,
      authRedirectToLogin = authRedirectToLogin,
      authCallbackUri = authCallbackUri,
      maxInboundMessageSize = maxInboundMessageSize,
      minRestartInterval = minRestartInterval,
      maxRestartInterval = maxRestartInterval,
      maxAuthCallbacks = maxAuthCallbacks,
      authCallbackTimeout = authCallbackTimeout,
      maxHttpEntityUploadSize = maxHttpEntityUploadSize,
      httpEntityUploadTimeout = httpEntityUploadTimeout,
      timeProviderType = timeProviderType,
      commandTtl = commandTtl,
      init = init,
      jdbcConfig = jdbcConfig,
      portFile = portFile,
      allowExistingSchema = allowExistingSchema,
      compilerConfig = compilerConfig,
      triggerConfig = triggerConfig,
      rootLoggingLevel = rootLoggingLevel,
      logEncoder = logEncoder,
      metricsReporter = metricsReporter,
      metricsReportingInterval = metricsReportingInterval,
    )
  }

  def loadConfig: Option[ServiceConfig] =
    loadFromConfigFile.cata(
      {
        case Right(cfg) => Some(cfg.toServiceConfig)
        case Left(ex) =>
          logger.error(
            s"Error loading trigger service config from file $configFile",
            ex.prettyPrint(),
          )
          None
      },
      Some(loadFromCliArgs),
    )
}

private[trigger] object Cli {

  val DefaultHttpPort: Int = 8088
  val DefaultMaxInboundMessageSize: Int = RunnerConfig.DefaultMaxInboundMessageSize
  val DefaultMinRestartInterval: FiniteDuration = FiniteDuration(5, duration.SECONDS)
  val DefaultMaxRestartInterval: FiniteDuration = FiniteDuration(60, duration.SECONDS)
  // Adds up to ~1GB with DefaultMaxInboundMessagesSize
  val DefaultMaxAuthCallbacks: Int = 250
  val DefaultAuthCallbackTimeout: FiniteDuration = FiniteDuration(1, duration.MINUTES)
  val DefaultMaxHttpEntityUploadSize: Long = RunnerConfig.DefaultMaxInboundMessageSize.toLong
  val DefaultHttpEntityUploadTimeout: FiniteDuration = FiniteDuration(1, duration.MINUTES)
  val DefaultCompilerConfig: Compiler.Config = Compiler.Config.Default
  val DefaultCommandTtl: FiniteDuration = FiniteDuration(30, duration.SECONDS)

  private[trigger] def redirectToLogin(value: String): AuthClient.RedirectToLogin = {
    value.toLowerCase match {
      case "yes" => AuthClient.RedirectToLogin.Yes
      case "no" => AuthClient.RedirectToLogin.No
      case "auto" => AuthClient.RedirectToLogin.Auto
      case s =>
        throw new IllegalArgumentException(s"value '$s' is not one of 'yes', 'no', or 'auto'.")
    }
  }

  implicit val redirectToLoginRead: scopt.Read[AuthClient.RedirectToLogin] =
    scopt.Read.reads(redirectToLogin)

  private[trigger] val Default = Cli(
    configFile = None,
    darPaths = Nil,
    address = cliopts.Http.defaultAddress,
    httpPort = DefaultHttpPort,
    ledgerHost = null,
    ledgerPort = 0,
    authInternalUri = None,
    authExternalUri = None,
    authBothUri = None,
    authRedirectToLogin = AuthClient.RedirectToLogin.No,
    authCallbackUri = None,
    maxInboundMessageSize = DefaultMaxInboundMessageSize,
    minRestartInterval = DefaultMinRestartInterval,
    maxRestartInterval = DefaultMaxRestartInterval,
    maxAuthCallbacks = Cli.DefaultMaxAuthCallbacks,
    authCallbackTimeout = Cli.DefaultAuthCallbackTimeout,
    maxHttpEntityUploadSize = DefaultMaxHttpEntityUploadSize,
    httpEntityUploadTimeout = DefaultHttpEntityUploadTimeout,
    timeProviderType = TimeProviderType.WallClock,
    commandTtl = Duration.ofSeconds(30L),
    init = false,
    jdbcConfig = None,
    portFile = None,
    allowExistingSchema = false,
    compilerConfig = DefaultCompilerConfig,
    triggerConfig = DefaultTriggerRunnerConfig,
    rootLoggingLevel = None,
    logEncoder = LogEncoder.Plain,
  )

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements")) // scopt builders
  private class OptionParser(supportedJdbcDriverNames: Set[String])
      extends scopt.OptionParser[Cli]("trigger-service") {
    head("trigger-service")

    opt[Option[File]]('c', "config")
      .text(
        "The application config file, this is the recommended way to run the service, individual cli-args are now deprecated"
      )
      .valueName("<file>")
      .action((file, cli) => cli.copy(configFile = file))

    opt[String]("dar")
      .optional()
      .unbounded()
      .action((f, c) => c.copy(darPaths = Paths.get(f) :: c.darPaths))
      .text("Path to the dar file containing the trigger.")

    cliopts.Http.serverParse(this, serviceName = "Trigger")(
      address = (f, c) => c.copy(address = f(c.address)),
      httpPort = (f, c) => c.copy(httpPort = f(c.httpPort)),
      defaultHttpPort = Some(DefaultHttpPort),
      portFile = Some((f, c) => c.copy(portFile = f(c.portFile))),
    )

    opt[String]("ledger-host")
      .optional()
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname.")

    opt[Int]("ledger-port")
      .optional()
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port.")

    opt[String]("auth")
      .optional()
      .action((t, c) => c.copy(authBothUri = Some(Uri(t))))
      .text(
        "Sets both the internal and external auth URIs. Incompatible with --auth-internal and --auth-external."
      )

    opt[String]("auth-internal")
      .optional()
      .action((t, c) => c.copy(authInternalUri = Some(Uri(t))))
      .text(
        "Sets the internal auth URIs (used by the trigger service to connect directly to the middleware). Incompatible with --auth."
      )

    opt[String]("auth-external")
      .optional()
      .action((t, c) => c.copy(authExternalUri = Some(Uri(t))))
      .text(
        "Sets the external auth URI (the one returned to the browser). Incompatible with --auth."
      )

    opt[AuthClient.RedirectToLogin]("auth-redirect")
      .optional()
      .action((x, c) => c.copy(authRedirectToLogin = x))
      .text(
        "Redirect to auth middleware login endpoint when unauthorized. One of 'yes', 'no', or 'auto'."
      )

    opt[String]("auth-callback")
      .optional()
      .action((t, c) => c.copy(authCallbackUri = Some(Uri(t))))
      .text(
        "URI to the auth login flow callback endpoint `/cb`. By default constructed from the incoming login request."
      )

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${DefaultMaxInboundMessageSize}."
      )

    opt[Long]("min-restart-interval")
      .action((x, c) => c.copy(minRestartInterval = FiniteDuration(x, duration.SECONDS)))
      .optional()
      .text(
        s"Minimum time interval before restarting a failed trigger. Defaults to ${DefaultMinRestartInterval.toSeconds} seconds."
      )

    opt[Long]("max-restart-interval")
      .action((x, c) => c.copy(maxRestartInterval = FiniteDuration(x, duration.SECONDS)))
      .optional()
      .text(
        s"Maximum time interval between restarting a failed trigger. Defaults to ${DefaultMaxRestartInterval.toSeconds} seconds."
      )

    opt[Int]("max-pending-authorizations")
      .action((x, c) => c.copy(maxAuthCallbacks = x))
      .optional()
      .text(
        s"Optional max number of pending authorization requests. Defaults to ${DefaultMaxAuthCallbacks}."
      )

    opt[Long]("authorization-timeout")
      .action((x, c) => c.copy(authCallbackTimeout = FiniteDuration(x, duration.SECONDS)))
      .optional()
      .text(
        s"Optional authorization timeout. Defaults to ${DefaultAuthCallbackTimeout.toSeconds} seconds."
      )

    opt[Long]("max-http-entity-upload-size")
      .action((x, c) => c.copy(maxHttpEntityUploadSize = x))
      .optional()
      .text(s"Optional max HTTP entity upload size. Defaults to ${DefaultMaxHttpEntityUploadSize}.")

    opt[Long]("http-entity-upload-timeout")
      .action((x, c) => c.copy(httpEntityUploadTimeout = FiniteDuration(x, duration.SECONDS)))
      .optional()
      .text(
        s"Optional HTTP entity upload timeout. Defaults to ${DefaultHttpEntityUploadTimeout.toSeconds} seconds."
      )

    opt[Unit]('s', "static-time")
      .optional()
      .action((_, c) => c.copy(timeProviderType = TimeProviderType.Static))
      .text("Use static time. When not specified, wall-clock time is used.")

    opt[Unit]('w', "wall-clock-time")
      .optional()
      .text(
        "[DEPRECATED] Wall-clock time is the default. This flag has no effect. Use `-s` to enable static time."
      )

    opt[Long]("ttl")
      .action { (t, c) =>
        c.copy(commandTtl = Duration.ofSeconds(t))
      }
      .text("TTL in seconds used for commands emitted by the trigger. Defaults to 30s.")

    opt[Unit]('v', "verbose")
      .text("Root logging level -> DEBUG")
      .action((_, cli) => cli.copy(rootLoggingLevel = Some(Level.DEBUG)))

    opt[Unit]("debug")
      .text("Root logging level -> DEBUG")
      .action((_, cli) => cli.copy(rootLoggingLevel = Some(Level.DEBUG)))

    implicit val levelRead: scopt.Read[Level] = scopt.Read.reads(Level.valueOf)
    opt[Level]("log-level-root")
      .text("Log-level of the root logger")
      .valueName("<LEVEL>")
      .action((level, cli) => cli.copy(rootLoggingLevel = Some(level)))

    opt[String]("log-encoder")
      .text("Log encoder: plain|json")
      .action {
        case ("json", cli) => cli.copy(logEncoder = LogEncoder.Json)
        case ("plain", cli) => cli.copy(logEncoder = LogEncoder.Plain)
        case (other, _) => throw new IllegalArgumentException(s"Unsupported logging encoder $other")
      }

    opt[Long]("max-batch-size")
      .optional()
      .text(
        s"Maximum number of messages triggers will batch (for rule evaluation/processing). Defaults to ${DefaultTriggerRunnerConfig.maximumBatchSize}"
      )
      .action((size, cli) =>
        if (size > 0) cli.copy(triggerConfig = cli.triggerConfig.copy(maximumBatchSize = size))
        else throw new IllegalArgumentException("batch size must be strictly positive")
      )

    opt[FiniteDuration]("batch-duration")
      .optional()
      .text(
        s"Period of time we will wait before emitting a message batch (for rule evaluation/processing). Defaults to ${DefaultTriggerRunnerConfig.batchingDuration}"
      )
      .action((period, cli) =>
        cli.copy(triggerConfig = cli.triggerConfig.copy(batchingDuration = period))
      )

    opt[Long]("max-active-contracts")
      .optional()
      .text(
        s"maximum number of active contracts that triggers may process. Defaults to ${DefaultTriggerRunnerConfig.maximumActiveContracts}"
      )
      .action((size, cli) =>
        if (size > 0)
          cli.copy(triggerConfig = cli.triggerConfig.copy(maximumActiveContracts = size))
        else throw new IllegalArgumentException("active contract size must be strictly positive")
      )

    opt[Int]("overflow-size")
      .optional()
      .text(
        s"maximum number of in-flight command submissions before a trigger overflow exception occurs. Defaults to ${DefaultTriggerRunnerConfig.inFlightCommandOverflowCount}"
      )
      .action((size, cli) =>
        if (size > 0)
          cli.copy(triggerConfig = cli.triggerConfig.copy(inFlightCommandOverflowCount = size))
        else throw new IllegalArgumentException("overflow size must be strictly positive")
      )

    opt[Unit]("no-overflow")
      .optional()
      .text(
        "disables in-flight command overflow checks."
      )
      .action((_, cli) =>
        cli.copy(triggerConfig = cli.triggerConfig.copy(allowInFlightCommandOverflows = false))
      )

    opt[Unit]("dev-mode-unsafe")
      .action((_, c) => c.copy(compilerConfig = Compiler.Config.Dev))
      .optional()
      .text(
        "Turns on development mode. Development mode allows development versions of Daml-LF language."
      )
      .hidden()

    implicit val jcd: DBConfig.JdbcConfigDefaults = DBConfig.JdbcConfigDefaults(
      supportedJdbcDrivers = supportedJdbcDriverNames,
      defaultDriver = Some("org.postgresql.Driver"),
    )

    opt[Map[String, String]]("jdbc")
      .action { (x, c) =>
        c.copy(jdbcConfig =
          Some(
            JdbcConfig
              .create(x)
              .fold(e => throw new IllegalArgumentException(e), identity)
          )
        )
      }
      .optional()
      .text(JdbcConfig.help())
      .text(
        "JDBC configuration parameters. If omitted the service runs without a database. "
          + JdbcConfig.help()
      )

    opt[Boolean]("allow-existing-schema")
      .action((x, c) => c.copy(allowExistingSchema = x))
      .text(
        "Do not abort if there are existing tables in the database schema. EXPERT ONLY. Defaults to false."
      )

    cliopts.Metrics.metricsReporterParse(this)(
      (f, c) => c.copy(metricsReporter = f(c.metricsReporter)),
      (f, c) => c.copy(metricsReportingInterval = f(c.metricsReportingInterval)),
    )

    checkConfig { cfg =>
      if (
        (cfg.authBothUri.nonEmpty && (cfg.authInternalUri.nonEmpty || cfg.authExternalUri.nonEmpty))
        || (cfg.authInternalUri.nonEmpty != cfg.authExternalUri.nonEmpty)
      )
        failure("You must specify either just --auth or both --auth-internal and --auth-external.")
      else
        success
    }

    checkConfig { cfg =>
      if (cfg.configFile.isEmpty && (cfg.ledgerHost == null || cfg.ledgerPort == 0))
        failure(
          "Missing required values i.e --ledger-host and/or --ledger-port values for cli args are missing"
        )
      else
        success
    }

    checkConfig { cfg =>
      if (cfg.configFile.isDefined && (cfg.ledgerHost != null || cfg.ledgerPort != 0))
        Left("Found both config file and cli opts for the app, please provide only one of them")
      else Right(())
    }

    cmd("init-db")
      .action((_, c) => c.copy(init = true))
      .text("Initialize database and terminate.")

    help("help").text("Print this usage text")

  }

  def parse(args: Array[String], supportedJdbcDriverNames: Set[String]): Option[Cli] = {
    new OptionParser(supportedJdbcDriverNames).parse(args, Default)
  }

  def parseConfig(
      args: Array[String],
      supportedJdbcDriverNames: Set[String],
  ): Option[ServiceConfig] = {
    val cli = parse(args, supportedJdbcDriverNames)
    cli.flatMap(_.loadConfig)
  }
}
