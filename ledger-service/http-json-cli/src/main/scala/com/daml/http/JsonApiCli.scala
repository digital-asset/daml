// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.cliopts.Logging.LogEncoder
import com.daml.http.dbbackend.JdbcConfig
import com.daml.ledger.api.tls.TlsConfiguration
import java.io.File
import java.nio.file.Path
import scala.concurrent.duration._
import ch.qos.logback.classic.{Level => LogLevel}
import com.daml.metrics.api.reporters.MetricsReporter
import com.typesafe.scalalogging.StrictLogging
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import scalaz.syntax.std.option._

private[http] final case class JsonApiCli(
    configFile: Option[File],
    ledgerHost: String,
    ledgerPort: Int,
    address: String = com.daml.cliopts.Http.defaultAddress,
    httpPort: Int,
    portFile: Option[Path] = None,
    packageReloadInterval: FiniteDuration = StartSettings.DefaultPackageReloadInterval,
    packageMaxInboundMessageSize: Option[Int] = None,
    maxInboundMessageSize: Int = StartSettings.DefaultMaxInboundMessageSize,
    healthTimeoutSeconds: Int = StartSettings.DefaultHealthTimeoutSeconds,
    tlsConfig: TlsConfiguration = TlsConfiguration(enabled = false, None, None, None),
    jdbcConfig: Option[JdbcConfig] = None,
    staticContentConfig: Option[StaticContentConfig] = None,
    allowNonHttps: Boolean = false,
    wsConfig: Option[WebsocketConfig] = None,
    nonRepudiation: nonrepudiation.Configuration.Cli = nonrepudiation.Configuration.Cli.Empty,
    logLevel: Option[LogLevel] = None, // the default is in logback.xml
    logEncoder: LogEncoder = LogEncoder.Plain,
    metricsReporter: Option[MetricsReporter] = None,
    metricsReportingInterval: FiniteDuration = 10.seconds,
    surrogateTpIdCacheMaxEntries: Option[Long] = None,
) extends StartSettings
    with StrictLogging {

  def loadFromConfigFile: Option[Either[ConfigReaderFailures, FileBasedConfig]] =
    configFile.map(cf => ConfigSource.file(cf).load[FileBasedConfig])

  def loadFromCliArgs: Config = {
    Config(
      address = address,
      httpPort = httpPort,
      portFile = portFile,
      ledgerHost = ledgerHost,
      ledgerPort = ledgerPort,
      packageReloadInterval = packageReloadInterval,
      packageMaxInboundMessageSize = packageMaxInboundMessageSize,
      maxInboundMessageSize = maxInboundMessageSize,
      healthTimeoutSeconds = healthTimeoutSeconds,
      tlsConfig = tlsConfig,
      jdbcConfig = jdbcConfig,
      staticContentConfig = staticContentConfig,
      allowNonHttps = allowNonHttps,
      wsConfig = wsConfig,
      nonRepudiation = nonRepudiation,
      logLevel = logLevel,
      logEncoder = logEncoder,
      metricsReporter = metricsReporter,
      metricsReportingInterval = metricsReportingInterval,
      surrogateTpIdCacheMaxEntries = surrogateTpIdCacheMaxEntries,
    )
  }

  def loadConfig: Option[Config] =
    loadFromConfigFile.cata(
      {
        case Right(fileBasedConfig) =>
          Some(
            fileBasedConfig.toConfig(
              nonRepudiation,
              logLevel,
              logEncoder,
            )
          )
        case Left(ex) =>
          logger.error(
            s"Error loading json-api service config from file ${configFile}. Failure: ${ex.prettyPrint()}"
          )
          None
      },
      Some(loadFromCliArgs),
    )
}

private[http] object JsonApiCli {
  val Default = JsonApiCli(configFile = None, ledgerHost = "", ledgerPort = -1, httpPort = -1)
}
