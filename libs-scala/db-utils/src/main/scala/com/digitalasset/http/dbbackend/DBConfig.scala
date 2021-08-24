// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.typesafe.scalalogging.StrictLogging
import scalaz.std.option._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{@@, Show, Tag, \/}

import java.io.File
import scala.util.Try
import scalaz.\/.right

object DBConfig {
  type SupportedJdbcDriverNames = Set[String] @@ SupportedJdbcDrivers
  sealed trait SupportedJdbcDrivers
  val SupportedJdbcDrivers = Tag.of[SupportedJdbcDrivers]
}

final case class JdbcConfig(
    driver: String,
    url: String,
    user: String,
    password: String,
    createSchema: Boolean = false,
    tablePrefix: String = "",
    dbStartupMode: DbStartupMode = DbStartupMode.StartOnly,
    minIdle: Int = JdbcConfig.MinIdle,
    connectionTimeout: Long = JdbcConfig.ConnectionTimeout,
    idleTimeout: Long = JdbcConfig.IdleTimeout,
    backendSpecificConf: Queries.BackendSpecificConf = false,
)

abstract class ConfigCompanion[A, ReadCtx](name: String) {
  import com.daml.scalautil.ExceptionOps._

  protected val indent: String = List.fill(8)(" ").mkString

  protected[this] def create(x: Map[String, String], defaultDriver: Option[String])(implicit
      readCtx: ReadCtx
  ): Either[String, A]

  implicit final def `read instance`(implicit ctx: ReadCtx): scopt.Read[A] =
    scopt.Read.reads { s =>
      val x = implicitly[scopt.Read[Map[String, String]]].reads(s)
      create(x, None).fold(e => throw new IllegalArgumentException(e), identity)
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

object JdbcConfig
    extends ConfigCompanion[JdbcConfig, DBConfig.SupportedJdbcDriverNames]("JdbcConfig")
    with StrictLogging {

  final val MinIdle = 8
  final val IdleTimeout = 10000L // ms, minimum according to log, defaults to 600s
  final val ConnectionTimeout = 5000L

  implicit val showInstance: Show[JdbcConfig] = Show.shows(a =>
    s"JdbcConfig(driver=${a.driver}, url=${a.url}, user=${a.user}, start-mode=${a.dbStartupMode})"
  )

  private[this] val WorkaroundQueryTokenTooLong = "workaroundQueryTokenTooLong"

  def help(implicit supportedJdbcDriverNames: DBConfig.SupportedJdbcDriverNames): String =
    "Contains comma-separated key-value pairs. Where:\n" +
      s"${indent}driver -- JDBC driver class name, ${supportedJdbcDriverNames.unwrap.mkString(", ")} supported right now,\n" +
      s"${indent}url -- JDBC connection URL,\n" +
      s"${indent}user -- database user name,\n" +
      s"${indent}password -- database user password,\n" +
      s"${indent}tablePrefix -- prefix for table names to avoid collisions, empty by default,\n" +
      s"${indent}createSchema -- boolean flag, if set to true, the process will re-create database schema and terminate immediately. This is deprecated and replaced by start-mode, however if set it will always overrule start-mode.\n" +
      s"${indent}start-mode -- option setting how the schema should be handled. Valid options are ${DbStartupMode.allConfigValues
        .mkString(",")}.\n" +
      (if (supportedJdbcDriverNames.unwrap exists (_ contains "oracle"))
      s"${indent}$WorkaroundQueryTokenTooLong -- if true, use a slower schema on Oracle that supports querying with literals >256 bytes (DRG-50943)"
      else "") +
      s"${indent}Example: " + helpString(
        "org.postgresql.Driver",
        "jdbc:postgresql://localhost:5432/test?&ssl=true",
        "postgres",
        "password",
        "tablePrefix",
        "create-only",
      )

  lazy val usage: String = helpString(
    "<JDBC driver class name>",
    "<JDBC connection url>",
    "<user>",
    "<password>",
    "<tablePrefix>",
    s"<${DbStartupMode.allConfigValues.mkString("|")}>",
  )

  override def create(x: Map[String, String], defaultDriver: Option[String] = None)(implicit
      readCtx: DBConfig.SupportedJdbcDriverNames
  ): Either[String, JdbcConfig] =
    for {
      driver <-
        if (defaultDriver.isDefined) Right(x.getOrElse("driver", defaultDriver.get))
        else requiredField(x)("driver")
      DBConfig.SupportedJdbcDrivers(supportedJdbcDriverNames) = readCtx
      _ <- Either.cond(
        supportedJdbcDriverNames(driver),
        (),
        s"$driver unsupported.  Supported drivers: ${supportedJdbcDriverNames.mkString(", ")}",
      )
      url <- requiredField(x)("url")
      user <- requiredField(x)("user")
      password <- requiredField(x)("password")
      tablePrefix <- optionalStringField(x)("tablePrefix")
      createSchema <- optionalBooleanField(x)("createSchema").map(
        _.map { createSchema =>
          import DbStartupMode._
          logger.warn(
            s"The option 'createSchema' is deprecated. Please use 'start-mode=${getConfigValue(CreateOnly)}' for 'createSchema=true' and 'start-mode=${getConfigValue(StartOnly)}'  for 'createSchema=false'"
          )
          if (createSchema) CreateOnly else StartOnly
        }: Option[DbStartupMode]
      )
      dbStartupMode <- DbStartupMode.optionalSchemaHandlingField(x)("start-mode")
      workaroundQueryTokenTooLong <- optionalBooleanField(x)(WorkaroundQueryTokenTooLong)
    } yield JdbcConfig(
      driver = driver,
      url = url,
      user = user,
      password = password,
      tablePrefix = tablePrefix getOrElse "",
      dbStartupMode = createSchema orElse dbStartupMode getOrElse DbStartupMode.StartOnly,
      workaroundQueryTokenTooLong = workaroundQueryTokenTooLong getOrElse false,
    )

  private def helpString(
      driver: String,
      url: String,
      user: String,
      password: String,
      tablePrefix: String,
      dbStartupMode: String,
  ): String =
    s"""\"driver=$driver,url=$url,user=$user,password=$password,tablePrefix=$tablePrefix,start-mode=$dbStartupMode\""""
}
