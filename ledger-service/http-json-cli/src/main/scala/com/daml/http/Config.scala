// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import org.apache.pekko.stream.ThrottleMode
import com.daml.dbutils.ConfigCompanion
import com.daml.ledger.api.tls.TlsConfiguration
import scalaz.std.either._
import scalaz.Show
import scalaz.StateT.liftM

import scala.concurrent.duration._
import ch.qos.logback.classic.{Level => LogLevel}
import com.daml.cliopts.Logging.LogEncoder
import com.daml.http.{WebsocketConfig => WSC}
import com.daml.http.dbbackend.JdbcConfig
import com.daml.metrics.{HistogramDefinition, MetricsConfig}
import com.daml.metrics.api.reporters.MetricsReporter

// The internal transient scopt structure *and* StartSettings; external `start`
// users should extend StartSettings or DefaultStartSettings themselves
private[http] final case class Config(
    ledgerHost: String,
    ledgerPort: Int,
    address: String = com.daml.cliopts.Http.defaultAddress,
    httpPort: Int,
    https: Option[TlsConfiguration] = None,
    portFile: Option[Path] = None,
    packageReloadInterval: FiniteDuration = StartSettings.DefaultPackageReloadInterval,
    packageMaxInboundMessageSize: Option[Int] = None,
    maxInboundMessageSize: Int = StartSettings.DefaultMaxInboundMessageSize,
    healthTimeoutSeconds: Int = StartSettings.DefaultHealthTimeoutSeconds,
    tlsConfig: TlsConfiguration = TlsConfiguration(enabled = false, None, None, None),
    jdbcConfig: Option[JdbcConfig] = None,
    staticContentConfig: Option[StaticContentConfig] = None,
    authConfig: Option[AuthConfig] = None,
    allowNonHttps: Boolean = false,
    wsConfig: Option[WebsocketConfig] = None,
    nonRepudiation: nonrepudiation.Configuration.Cli = nonrepudiation.Configuration.Cli.Empty,
    logLevel: Option[LogLevel] = None, // the default is in logback.xml
    logEncoder: LogEncoder = LogEncoder.Plain,
    metricsReporter: Option[MetricsReporter] = None,
    metricsReportingInterval: FiniteDuration = MetricsConfig.DefaultMetricsReportingInterval,
    surrogateTpIdCacheMaxEntries: Option[Long] = None,
    histograms: Seq[HistogramDefinition] = Seq.empty,
) extends StartSettings

private[http] object Config {
  val Empty = Config(ledgerHost = "", ledgerPort = -1, httpPort = -1)
}

// It is public for Daml Hub
final case class WebsocketConfig(
    maxDuration: FiniteDuration = WSC.DefaultMaxDuration,
    throttleElem: Int = WSC.DefaultThrottleElem,
    throttlePer: FiniteDuration = WSC.DefaultThrottlePer,
    maxBurst: Int = WSC.DefaultMaxBurst,
    mode: ThrottleMode = WSC.DefaultThrottleMode,
    heartbeatPeriod: FiniteDuration = WSC.DefaultHeartbeatPeriod,
)

private[http] object WebsocketConfig
    extends ConfigCompanion[WebsocketConfig, DummyImplicit]("WebsocketConfig") {

  implicit val showInstance: Show[WebsocketConfig] = Show.shows(c =>
    s"WebsocketConfig(maxDuration=${c.maxDuration}, heartBeatPer=${c.heartbeatPeriod})"
  )

  val DefaultMaxDuration: FiniteDuration = 120.minutes
  val DefaultThrottleElem: Int = 20
  val DefaultThrottlePer: FiniteDuration = 1.second
  val DefaultMaxBurst: Int = 20
  val DefaultThrottleMode: ThrottleMode = ThrottleMode.Shaping
  val DefaultHeartbeatPeriod: FiniteDuration = 5.second

  lazy val help: String =
    "Contains comma-separated key-value pairs. Where:\n" +
      s"${indent}maxDuration -- Maximum websocket session duration in minutes\n" +
      s"${indent}heartBeatPer -- Server-side heartBeat interval in seconds\n" +
      s"${indent}Example: " + helpString("120", "5")

  lazy val usage: String = helpString(
    "<Maximum websocket session duration in minutes>",
    "Server-side heartBeat interval in seconds",
  )

  protected[this] override def create(implicit readCtx: DummyImplicit) =
    for {
      md <- optionalLongField("maxDuration")
      hbp <- optionalLongField("heartBeatPer")
    } yield WebsocketConfig(
      maxDuration = md
        .map(t => FiniteDuration(t, TimeUnit.MINUTES))
        .getOrElse(WebsocketConfig.DefaultMaxDuration),
      heartbeatPeriod = hbp
        .map(t => FiniteDuration(t, TimeUnit.SECONDS))
        .getOrElse(WebsocketConfig.DefaultHeartbeatPeriod),
    )

  private def helpString(maxDuration: String, heartBeatPer: String): String =
    s"""\"maxDuration=$maxDuration,heartBeatPer=$heartBeatPer\""""
}

private[http] final case class StaticContentConfig(
    prefix: String,
    directory: File,
)

private[http] object StaticContentConfig
    extends ConfigCompanion[StaticContentConfig, DummyImplicit]("StaticContentConfig") {

  implicit val showInstance: Show[StaticContentConfig] =
    Show.shows(a => s"StaticContentConfig(prefix=${a.prefix}, directory=${a.directory})")

  lazy val help: String =
    "Contains comma-separated key-value pairs. Where:\n" +
      s"${indent}prefix -- URL prefix,\n" +
      s"${indent}directory -- local directory that will be mapped to the URL prefix.\n" +
      s"${indent}Example: " + helpString("static", "./static-content")

  lazy val usage: String = helpString("<URL prefix>", "<directory>")

  protected[this] override def create(implicit readCtx: DummyImplicit) =
    for {
      prefix <- requiredField("prefix").flatMap(p => liftM(prefixCantStartWithSlash(p)))
      directory <- requiredDirectoryField("directory")
    } yield StaticContentConfig(prefix, directory)

  private def prefixCantStartWithSlash(s: String): Either[String, String] =
    if (s.startsWith("/")) Left(s"prefix cannot start with slash: $s")
    else Right(s)

  private def helpString(prefix: String, directory: String): String =
    s"""\"prefix=$prefix,directory=$directory\""""
}

private[http] final case class AuthConfig(
    targetScope: Option[String]
)

private[http] object AuthConfig extends ConfigCompanion[AuthConfig, DummyImplicit]("AuthConfig") {

  implicit val showInstance: Show[AuthConfig] =
    Show.shows(a => s"AuthConfig(targetScope=${a.targetScope})")

  protected[this] override def create(implicit readCtx: DummyImplicit) =
    for {
      targetScope <- optionalStringField("targetScope")
    } yield AuthConfig(targetScope)
}
