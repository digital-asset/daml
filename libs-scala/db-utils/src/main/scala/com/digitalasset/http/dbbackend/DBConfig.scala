// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.typesafe.scalalogging.StrictLogging
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.std.option._
import scalaz.syntax.tag._
import scalaz.syntax.traverse._
import scalaz.{@@, Show, StateT, Tag, \/}

import java.io.File
import scala.util.Try

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
    backendSpecificConf: Map[String, String] = Map.empty,
)

abstract class ConfigCompanion[A, ReadCtx](name: String) {
  import com.daml.scalautil.ExceptionOps._

  protected val indent: String = List.fill(8)(" ").mkString

  // If we don't DRY our keys, we will definitely forget to remove one.  We're
  // less likely to make a mistake in backend-specific conf if redundant data
  // isn't there. -SC
  protected type Fields[Z] = StateT[({ type l[a] = Either[String, a] })#l, Map[String, String], Z]

  protected[this] def create(x: Map[String, String], defaultDriver: Option[String])(implicit
      readCtx: ReadCtx
  ): Either[String, A]

  implicit final def `read instance`(implicit ctx: ReadCtx): scopt.Read[A] =
    scopt.Read.reads { s =>
      val x = implicitly[scopt.Read[Map[String, String]]].reads(s)
      create(x, None).fold(e => throw new IllegalArgumentException(e), identity)
    }

  protected def requiredField(k: String): Fields[String] =
    StateT { m =>
      m.get(k)
        .filter(_.nonEmpty)
        .map((m removed k, _))
        .toRight(s"Invalid $name, must contain '$k' field")
    }

  protected def optionalStringField(
      k: String
  ): Fields[Option[String]] =
    StateT { m => Right((m removed k, m get k)) }

  protected def optionalBooleanField(
      k: String
  ): Fields[Option[Boolean]] =
    optionalStringField(k).flatMap(ov => StateT liftM (ov traverse parseBoolean(k)).toEither)

  protected def optionalLongField(k: String): Fields[Option[Long]] =
    optionalStringField(k).flatMap(ov => StateT liftM (ov traverse parseLong(k)).toEither)

  import scalaz.syntax.std.string._

  protected def parseBoolean(k: String)(v: String): String \/ Boolean =
    v.parseBoolean.leftMap(e => s"$k=$v must be a boolean value: ${e.description}").disjunction

  protected def parseLong(k: String)(v: String): String \/ Long =
    v.parseLong.leftMap(e => s"$k=$v must be a int value: ${e.description}").disjunction

  protected def requiredDirectoryField(k: String): Fields[File] =
    requiredField(k).flatMap(s => StateT liftM directory(s))

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

  private[this] val WorkaroundQueryTokenTooLong = "disableContractPayloadIndexing"

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
  ): Either[String, JdbcConfig] = {
    val reducingX = for {
      driver <-
        defaultDriver.cata(
          dd => optionalStringField("driver").map(_ getOrElse dd),
          requiredField("driver"),
        )
      DBConfig.SupportedJdbcDrivers(supportedJdbcDriverNames) = readCtx
      _ <- StateT liftM Either.cond(
        supportedJdbcDriverNames(driver),
        (),
        s"$driver unsupported.  Supported drivers: ${supportedJdbcDriverNames.mkString(", ")}",
      )
      url <- requiredField("url")
      user <- requiredField("user")
      password <- requiredField("password")
      tablePrefix <- optionalStringField("tablePrefix")
      createSchema <- optionalBooleanField("createSchema").map(
        _.map { createSchema =>
          import DbStartupMode._
          logger.warn(
            s"The option 'createSchema' is deprecated. Please use 'start-mode=${getConfigValue(CreateOnly)}' for 'createSchema=true' and 'start-mode=${getConfigValue(StartOnly)}'  for 'createSchema=false'"
          )
          if (createSchema) CreateOnly else StartOnly
        }: Option[DbStartupMode]
      )
      dbStartupMode <- optionalStringField("start-mode").flatMap { osm =>
        StateT liftM osm.traverse(DbStartupMode.parseSchemaHandlingField)
      }
      remainingConf <- StateT.get: Fields[Map[String, String]]
    } yield JdbcConfig(
      driver = driver,
      url = url,
      user = user,
      password = password,
      tablePrefix = tablePrefix getOrElse "",
      dbStartupMode = createSchema orElse dbStartupMode getOrElse DbStartupMode.StartOnly,
      backendSpecificConf = remainingConf,
    )
    reducingX.eval(x)
  }

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
