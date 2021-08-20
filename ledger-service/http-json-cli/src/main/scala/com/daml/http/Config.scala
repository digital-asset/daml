// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import akka.stream.ThrottleMode
import com.daml.scalautil.ExceptionOps._
import com.daml.ledger.api.tls.TlsConfiguration
import scalaz.std.option._
import scalaz.syntax.traverse._
import scalaz.{@@, Show, Tag, \/}

import scala.concurrent.duration._
import scala.util.Try

import ch.qos.logback.classic.{Level => LogLevel}
import com.daml.cliopts.Logging.LogEncoder
import com.daml.metrics.MetricsReporter
import com.daml.http.dbbackend.JdbcConfig
import scalaz.\/.right

// The internal transient scopt structure *and* StartSettings; external `start`
// users should extend StartSettings or DefaultStartSettings themselves
private[http] final case class Config(
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
    metricsReportingInterval: FiniteDuration = 10 seconds,
) extends StartSettings

private[http] object Config {
  import scala.language.postfixOps
  val Empty = Config(ledgerHost = "", ledgerPort = -1, httpPort = -1)
  val DefaultWsConfig =
    WebsocketConfig(
      maxDuration = 120 minutes,
      throttleElem = 20,
      throttlePer = 1 second,
      maxBurst = 20,
      ThrottleMode.Shaping,
      heartBeatPer = 5 second,
    )

  type SupportedJdbcDriverNames = Set[String] @@ SupportedJdbcDrivers
  sealed trait SupportedJdbcDrivers
  val SupportedJdbcDrivers = Tag.of[SupportedJdbcDrivers]
}

private[http] abstract class ConfigCompanion[A, ReadCtx](name: String) {

  protected val indent: String = List.fill(8)(" ").mkString

  protected[this] def create(x: Map[String, String])(implicit readCtx: ReadCtx): Either[String, A]

  private[http] implicit final def `read instance`(implicit ctx: ReadCtx): scopt.Read[A] =
    scopt.Read.reads { s =>
      val x = implicitly[scopt.Read[Map[String, String]]].reads(s)
      create(x).fold(e => throw new IllegalArgumentException(e), identity)
    }

  protected def requiredField(m: Map[String, String])(k: String): Either[String, String] =
    m.get(k).filter(_.nonEmpty).toRight(s"Invalid $name, must contain '$k' field")

  protected def optionalStringField(m: Map[String, String])(
      k: String
  ): Either[String, Option[String]] =
    right(m.get(k)).toEither

  protected def optionalBooleanField(m: Map[String, String])(
      k: String
  ): Either[String, Option[Boolean]] =
    m.get(k).traverse(v => parseBoolean(k)(v)).toEither

  protected def optionalLongField(m: Map[String, String])(k: String): Either[String, Option[Long]] =
    m.get(k).traverse(v => parseLong(k)(v)).toEither

  import scalaz.syntax.std.string._

  protected def parseBoolean(k: String)(v: String): String \/ Boolean =
    v.parseBoolean.leftMap(e => s"$k=$v must be a boolean value: ${e.description}").disjunction

  protected def parseLong(k: String)(v: String): String \/ Long =
    v.parseLong.leftMap(e => s"$k=$v must be a int value: ${e.description}").disjunction

  protected def requiredDirectoryField(m: Map[String, String])(k: String): Either[String, File] =
    requiredField(m)(k).flatMap(directory)

  protected def directory(s: String): Either[String, File] =
    Try(new File(s).getAbsoluteFile).toEither.left
      .map(e => e.description)
      .flatMap { d =>
        if (d.isDirectory) Right(d)
        else Left(s"Directory does not exist: ${d.getAbsolutePath}")
      }
}

// It is public for Daml Hub
final case class WebsocketConfig(
    maxDuration: FiniteDuration,
    throttleElem: Int,
    throttlePer: FiniteDuration,
    maxBurst: Int,
    mode: ThrottleMode,
    heartBeatPer: FiniteDuration,
)

private[http] object WebsocketConfig
    extends ConfigCompanion[WebsocketConfig, DummyImplicit]("WebsocketConfig") {

  implicit val showInstance: Show[WebsocketConfig] = Show.shows(c =>
    s"WebsocketConfig(maxDuration=${c.maxDuration}, heartBeatPer=${c.heartBeatPer}.seconds)"
  )

  lazy val help: String =
    "Contains comma-separated key-value pairs. Where:\n" +
      s"${indent}maxDuration -- Maximum websocket session duration in minutes\n" +
      s"${indent}heartBeatPer -- Server-side heartBeat interval in seconds\n" +
      s"${indent}Example: " + helpString("120", "5")

  lazy val usage: String = helpString(
    "<Maximum websocket session duration in minutes>",
    "Server-side heartBeat interval in seconds",
  )

  override def create(
      x: Map[String, String]
  )(implicit readCtx: DummyImplicit): Either[String, WebsocketConfig] =
    for {
      md <- optionalLongField(x)("maxDuration")
      hbp <- optionalLongField(x)("heartBeatPer")
    } yield Config.DefaultWsConfig
      .copy(
        maxDuration = md
          .map(t => FiniteDuration(t, TimeUnit.MINUTES))
          .getOrElse(Config.DefaultWsConfig.maxDuration),
        heartBeatPer = hbp
          .map(t => FiniteDuration(t, TimeUnit.SECONDS))
          .getOrElse(Config.DefaultWsConfig.heartBeatPer),
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

  override def create(
      x: Map[String, String]
  )(implicit readCtx: DummyImplicit): Either[String, StaticContentConfig] =
    for {
      prefix <- requiredField(x)("prefix").flatMap(prefixCantStartWithSlash)
      directory <- requiredDirectoryField(x)("directory")
    } yield StaticContentConfig(prefix, directory)

  private def prefixCantStartWithSlash(s: String): Either[String, String] =
    if (s.startsWith("/")) Left(s"prefix cannot start with slash: $s")
    else Right(s)

  private def helpString(prefix: String, directory: String): String =
    s"""\"prefix=$prefix,directory=$directory\""""
}
