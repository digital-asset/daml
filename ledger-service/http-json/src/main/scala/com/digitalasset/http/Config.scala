// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.net.InetAddress
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import akka.stream.ThrottleMode
import com.daml.util.ExceptionOps._
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import scalaz.std.option._
import scalaz.syntax.traverse._
import scalaz.{Show, \/}

import scala.concurrent.duration._
import scala.util.Try

private[http] final case class Config(
    ledgerHost: String,
    ledgerPort: Int,
    address: String = InetAddress.getLoopbackAddress.getHostAddress,
    httpPort: Int,
    portFile: Option[Path] = None,
    applicationId: ApplicationId = ApplicationId("HTTP-JSON-API-Gateway"),
    packageReloadInterval: FiniteDuration = HttpService.DefaultPackageReloadInterval,
    maxInboundMessageSize: Int = HttpService.DefaultMaxInboundMessageSize,
    tlsConfig: TlsConfiguration = TlsConfiguration(enabled = false, None, None, None),
    jdbcConfig: Option[JdbcConfig] = None,
    staticContentConfig: Option[StaticContentConfig] = None,
    accessTokenFile: Option[Path] = None,
    wsConfig: Option[WebsocketConfig] = None,
)

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
      heartBeatPer = 5 second)
}

private[http] abstract class ConfigCompanion[A](name: String) {

  protected val indent: String = List.fill(8)(" ").mkString

  def create(x: Map[String, String]): Either[String, A]

  def createUnsafe(x: Map[String, String]): A = create(x).fold(
    e => sys.error(e),
    identity
  )

  def validate(x: Map[String, String]): Either[String, Unit] =
    create(x).map(_ => ())

  protected def requiredField(m: Map[String, String])(k: String): Either[String, String] =
    m.get(k).filter(_.nonEmpty).toRight(s"Invalid $name, must contain '$k' field")

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  protected def optionalBooleanField(m: Map[String, String])(
      k: String): Either[String, Option[Boolean]] =
    m.get(k).traverse(v => parseBoolean(k)(v)).toEither

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
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

private[http] final case class JdbcConfig(
    driver: String,
    url: String,
    user: String,
    password: String,
    createSchema: Boolean = false
)

private[http] object JdbcConfig extends ConfigCompanion[JdbcConfig]("JdbcConfig") {

  implicit val showInstance: Show[JdbcConfig] = Show.shows(a =>
    s"JdbcConfig(driver=${a.driver}, url=${a.url}, user=${a.user}, createSchema=${a.createSchema})")

  lazy val help: String =
    "Contains comma-separated key-value pairs. Where:\n" +
      s"${indent}driver -- JDBC driver class name, only org.postgresql.Driver supported right now,\n" +
      s"${indent}url -- JDBC connection URL, only jdbc:postgresql supported right now,\n" +
      s"${indent}user -- database user name,\n" +
      s"${indent}password -- database user password,\n" +
      s"${indent}createSchema -- boolean flag, if set to true, the process will re-create database schema and terminate immediately.\n" +
      s"${indent}Example: " + helpString(
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/test?&ssl=true",
      "postgres",
      "password",
      "false")

  lazy val usage: String = helpString(
    "<JDBC driver class name>",
    "<JDBC connection url>",
    "<user>",
    "<password>",
    "<true|false>")

  override def create(x: Map[String, String]): Either[String, JdbcConfig] =
    for {
      driver <- requiredField(x)("driver")
      url <- requiredField(x)("url")
      user <- requiredField(x)("user")
      password <- requiredField(x)("password")
      createSchema <- optionalBooleanField(x)("createSchema")
    } yield
      JdbcConfig(
        driver = driver,
        url = url,
        user = user,
        password = password,
        createSchema = createSchema.getOrElse(false)
      )

  private def helpString(
      driver: String,
      url: String,
      user: String,
      password: String,
      createSchema: String): String =
    s"""\"driver=$driver,url=$url,user=$user,password=$password,createSchema=$createSchema\""""
}

// It is public for DABL
final case class WebsocketConfig(
    maxDuration: FiniteDuration,
    throttleElem: Int,
    throttlePer: FiniteDuration,
    maxBurst: Int,
    mode: ThrottleMode,
    heartBeatPer: FiniteDuration
)

private[http] object WebsocketConfig extends ConfigCompanion[WebsocketConfig]("WebsocketConfig") {

  implicit val showInstance: Show[WebsocketConfig] = Show.shows(c =>
    s"WebsocketConfig(maxDuration=${c.maxDuration}, heartBeatPer=${c.heartBeatPer}.seconds)")

  lazy val help: String =
    "Contains comma-separated key-value pairs. Where:\n" +
      s"${indent}maxDuration -- Maximum websocket session duration in minutes\n" +
      s"${indent}heartBeatPer -- Server-side heartBeat interval in seconds\n" +
      s"${indent}Example: " + helpString("120", "5")

  lazy val usage: String = helpString(
    "<Maximum websocket session duration in minutes>",
    "Server-side heartBeat interval in seconds")

  override def create(x: Map[String, String]): Either[String, WebsocketConfig] =
    for {
      md <- optionalLongField(x)("maxDuration")
      hbp <- optionalLongField(x)("heartBeatPer")
    } yield
      Config.DefaultWsConfig
        .copy(
          maxDuration = md
            .map(t => FiniteDuration(t, TimeUnit.MINUTES))
            .getOrElse(Config.DefaultWsConfig.maxDuration),
          heartBeatPer = hbp
            .map(t => FiniteDuration(t, TimeUnit.SECONDS))
            .getOrElse(Config.DefaultWsConfig.heartBeatPer)
        )

  private def helpString(maxDuration: String, heartBeatPer: String): String =
    s"""\"maxDuration=$maxDuration,heartBeatPer=$heartBeatPer\""""
}

private[http] final case class StaticContentConfig(
    prefix: String,
    directory: File
)

private[http] object StaticContentConfig
    extends ConfigCompanion[StaticContentConfig]("StaticContentConfig") {

  implicit val showInstance: Show[StaticContentConfig] =
    Show.shows(a => s"StaticContentConfig(prefix=${a.prefix}, directory=${a.directory})")

  lazy val help: String =
    "Contains comma-separated key-value pairs. Where:\n" +
      s"${indent}prefix -- URL prefix,\n" +
      s"${indent}directory -- local directory that will be mapped to the URL prefix.\n" +
      s"${indent}Example: " + helpString("static", "./static-content")

  lazy val usage: String = helpString("<URL prefix>", "<directory>")

  override def create(x: Map[String, String]): Either[String, StaticContentConfig] =
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
