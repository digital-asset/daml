// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import scalaz.{Show, \/}
import scalaz.std.option._
import scalaz.syntax.traverse._

import scala.concurrent.duration.FiniteDuration

private[http] final case class Config(
    ledgerHost: String,
    ledgerPort: Int,
    address: String = "0.0.0.0",
    httpPort: Int,
    applicationId: ApplicationId = ApplicationId("HTTP-JSON-API-Gateway"),
    packageReloadInterval: FiniteDuration = HttpService.DefaultPackageReloadInterval,
    maxInboundMessageSize: Int = HttpService.DefaultMaxInboundMessageSize,
    jdbcConfig: Option[JdbcConfig] = None,
)

private[http] object Config {
  val Empty = Config(ledgerHost = "", ledgerPort = -1, httpPort = -1)
}

private[http] final case class JdbcConfig(
    driver: String,
    url: String,
    user: String,
    password: String,
    createSchema: Boolean = false
)

private[http] object JdbcConfig {

  implicit val showInstance: Show[JdbcConfig] = Show.shows(a =>
    s"JdbcConfig(driver=${a.driver}, url=${a.url}, user=${a.user}, createSchema=${a.createSchema})")

  lazy val help: String = helpString(
    "<JDBC driver class name>",
    "<JDBC connection url>",
    "<user>",
    "<password>",
    "<true | false>")

  lazy val example: String = helpString(
    "org.postgresql.Driver",
    "jdbc:postgresql://localhost:5432/test?&ssl=true",
    "postgres",
    "password",
    "false")

  def createUnsafe(x: Map[String, String]): JdbcConfig = create(x).fold(
    e => sys.error(e),
    identity
  )

  def create(x: Map[String, String]): Either[String, JdbcConfig] =
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

  def validate(x: Map[String, String]): Either[String, Unit] =
    for {
      _ <- requiredField(x)("driver")
      _ <- requiredField(x)("url")
      _ <- requiredField(x)("user")
      _ <- requiredField(x)("password")
      _ <- optionalBooleanField(x)("createSchema")
    } yield ()

  private def requiredField(m: Map[String, String])(k: String): Either[String, String] =
    m.get(k).toRight(s"Invalid JdbcConfig, must contain '$k' field}")

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def optionalBooleanField(m: Map[String, String])(
      k: String): Either[String, Option[Boolean]] =
    m.get(k).traverseU(v => parseBoolean(k)(v)).toEither

  private def parseBoolean(k: String)(v: String): String \/ Boolean =
    \/.fromTryCatchNonFatal(v.toBoolean).leftMap(e =>
      s"$k=$v must be a boolean value: ${e.getMessage}")

  private def helpString(
      driver: String,
      url: String,
      user: String,
      password: String,
      createSchema: String): String =
    s"driver=$driver,url=$url,user=$user,password=$password,createSchema=$createSchema"

}
