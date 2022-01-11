// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import akka.stream.ThrottleMode
import com.daml.cliopts
import com.daml.cliopts.Logging.LogEncoder
import com.daml.http.dbbackend.{DbStartupMode, JdbcConfig}
import com.daml.pureconfigutils.{HttpServerConfig, LedgerApiConfig, MetricsConfig}
import com.daml.pureconfigutils.SharedConfigReaders._
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._
import ch.qos.logback.classic.{Level => LogLevel}

import scala.concurrent.duration._

private[http] object FileBasedConfig {

  implicit val throttleModeCfgReader: ConfigReader[ThrottleMode] =
    ConfigReader.fromString[ThrottleMode](catchConvertError { s =>
      s.toLowerCase() match {
        case "enforcing" => Right(ThrottleMode.Enforcing)
        case "shaping" => Right(ThrottleMode.Shaping)
        case _ => Left("not one of 'shaping' or 'enforcing'")
      }
    })
  implicit val websocketCfgReader: ConfigReader[WebsocketConfig] =
    deriveReader[WebsocketConfig]
  implicit val staticContentCfgReader: ConfigReader[StaticContentConfig] =
    deriveReader[StaticContentConfig]

  implicit val dbStartupModeReader: ConfigReader[DbStartupMode] =
    ConfigReader.fromString[DbStartupMode](catchConvertError { s =>
      DbStartupMode.configValuesMap
        .get(s.toLowerCase())
        .toRight(
          s"not one of ${DbStartupMode.allConfigValues.mkString(",")}"
        )
    })
  implicit val queryStoreCfgReader: ConfigReader[JdbcConfig] = deriveReader[JdbcConfig]

  implicit val httpJsonApiCfgReader: ConfigReader[FileBasedConfig] =
    deriveReader[FileBasedConfig]

  val Empty: FileBasedConfig = FileBasedConfig(
    HttpServerConfig(cliopts.Http.defaultAddress, -1),
    LedgerApiConfig("", -1),
  )
}
private[http] final case class FileBasedConfig(
    server: HttpServerConfig,
    ledgerApi: LedgerApiConfig,
    queryStore: Option[JdbcConfig] = None,
    packageReloadInterval: FiniteDuration = StartSettings.DefaultPackageReloadInterval,
    maxInboundMessageSize: Int = StartSettings.DefaultMaxInboundMessageSize,
    healthTimeoutSeconds: Int = StartSettings.DefaultHealthTimeoutSeconds,
    packageMaxInboundMessageSize: Option[Int] = None,
    maxTemplateIdCacheEntries: Option[Long] = None,
    websocketConfig: Option[WebsocketConfig] = None,
    metrics: Option[MetricsConfig] = None,
    allowInsecureTokens: Boolean = false,
    staticContent: Option[StaticContentConfig] = None,
) {
  def toConfig(
      nonRepudiation: nonrepudiation.Configuration.Cli,
      logLevel: Option[LogLevel], // the default is in logback.xml
      logEncoder: LogEncoder,
  ): Config = {
    Config(
      ledgerHost = ledgerApi.address,
      ledgerPort = ledgerApi.port,
      address = server.address,
      httpPort = server.port,
      portFile = server.portFile,
      packageReloadInterval = packageReloadInterval,
      packageMaxInboundMessageSize = packageMaxInboundMessageSize,
      maxInboundMessageSize = maxInboundMessageSize,
      healthTimeoutSeconds = healthTimeoutSeconds,
      tlsConfig = ledgerApi.tls.tlsConfiguration,
      jdbcConfig = queryStore,
      staticContentConfig = staticContent,
      allowNonHttps = allowInsecureTokens,
      wsConfig = websocketConfig,
      nonRepudiation = nonRepudiation,
      logLevel = logLevel,
      logEncoder = logEncoder,
      metricsReporter = metrics.map(_.reporter),
      metricsReportingInterval =
        metrics.map(_.reportingInterval).getOrElse(StartSettings.DefaultMetricsReportingInterval),
      surrogateTpIdCacheMaxEntries = maxTemplateIdCacheEntries,
    )
  }
}
