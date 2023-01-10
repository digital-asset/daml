// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.dbutils

import ConnectionPool.PoolSize
import com.typesafe.scalalogging.StrictLogging
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{Show, StateT, \/}

import java.io.File
import scala.concurrent.duration._
import scala.util.Try

object DBConfig {
  final case class JdbcConfigDefaults(
      supportedJdbcDrivers: Set[String],
      defaultDriver: Option[String] = None,
  )
}

final case class JdbcConfig(
    driver: String,
    url: String,
    user: String,
    password: String,
    poolSize: Int,
    minIdle: Int = JdbcConfig.MinIdle,
    connectionTimeout: FiniteDuration = JdbcConfig.ConnectionTimeout,
    idleTimeout: FiniteDuration = JdbcConfig.IdleTimeout,
    tablePrefix: String = "",
)

abstract class ConfigCompanion[A, ReadCtx](name: String) {
  import com.daml.scalautil.ExceptionOps._

  protected val indent: String = List.fill(8)(" ").mkString

  // If we don't DRY our keys, we will definitely forget to remove one.  We're
  // less likely to make a mistake in backend-specific conf if redundant data
  // isn't there. -SC
  type Fields[Z] = StateT[({ type l[a] = Either[String, a] })#l, Map[String, String], Z]

  protected[this] def create(implicit
      readCtx: ReadCtx
  ): Fields[A]

  implicit final def `read instance`(implicit ctx: ReadCtx): scopt.Read[A] =
    scopt.Read.reads { s =>
      val x = implicitly[scopt.Read[Map[String, String]]].reads(s)
      create.eval(x).fold(e => throw new IllegalArgumentException(e), identity)
    }

  protected def requiredField(k: String): Fields[String] =
    StateT { m =>
      m.get(k)
        .filter(_.nonEmpty)
        .map((m - k, _))
        .toRight(s"Invalid $name, must contain '$k' field")
    }

  protected def optionalStringField(
      k: String
  ): Fields[Option[String]] =
    StateT { m => Right((m - k, m get k)) }

  protected def optionalBooleanField(
      k: String
  ): Fields[Option[Boolean]] =
    optionalStringField(k).flatMap(ov => StateT liftM (ov traverse parseBoolean(k)).toEither)

  protected def optionalIntField(k: String): Fields[Option[Int]] =
    optionalStringField(k).flatMap(ov => StateT liftM (ov traverse parseInt(k)).toEither)

  protected def optionalLongField(k: String): Fields[Option[Long]] =
    optionalStringField(k).flatMap(ov => StateT liftM (ov traverse parseLong(k)).toEither)

  import scalaz.syntax.std.string._

  protected def parseBoolean(k: String)(v: String): String \/ Boolean =
    v.parseBoolean.leftMap(e => s"$k=$v must be a boolean value: ${e.description}").disjunction

  protected def parseLong(k: String)(v: String): String \/ Long =
    v.parseLong.leftMap(e => s"$k=$v must be a int value: ${e.description}").disjunction

  protected def parseInt(k: String)(v: String): String \/ Int =
    v.parseInt.leftMap(e => s"$k=$v must be an int value: ${e.description}").disjunction

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
    extends ConfigCompanion[JdbcConfig, DBConfig.JdbcConfigDefaults]("JdbcConfig")
    with StrictLogging {

  final val MinIdle = 8
  final val IdleTimeout = 10000.millis // minimum according to log, defaults to 600s
  final val ConnectionTimeout = 5000.millis

  @scala.deprecated("do I need this?", since = "SC")
  implicit val showInstance: Show[JdbcConfig] =
    Show.shows(a =>
      s"JdbcConfig(driver=${a.driver}, url=${a.url}, user=${a.user}, poolSize=${a.poolSize}, " +
        s"minIdle=${a.minIdle}, connectionTimeout=${a.connectionTimeout}, idleTimeout=${a.idleTimeout}"
    )

  def help(otherOptions: String = "")(implicit jcd: DBConfig.JdbcConfigDefaults): String =
    "Contains comma-separated key-value pairs. Where:\n" +
      s"${indent}driver -- JDBC driver class name, ${jcd.supportedJdbcDrivers.mkString(", ")} supported right now,\n" +
      s"${indent}url -- JDBC connection URL,\n" +
      s"${indent}user -- database user name,\n" +
      s"${indent}password -- database user password,\n" +
      s"${indent}tablePrefix -- prefix for table names to avoid collisions, empty by default,\n" +
      s"${indent}poolSize -- int value, specifies the max pool size for the database connection pool.\n" +
      s"${indent}minIdle -- int value, specifies the min idle connections for database connection pool.\n" +
      s"${indent}connectionTimeout -- long value, specifies the connection timeout for database connection pool.\n" +
      s"${indent}idleTimeout -- long value, specifies the idle timeout for the database connection pool.\n" +
      otherOptions +
      s"${indent}Example: " + helpString(
        "org.postgresql.Driver",
        "jdbc:postgresql://localhost:5432/test?&ssl=true",
        "postgres",
        "password",
        "table_prefix_",
        PoolSize.Production.toString,
        MinIdle.toString,
        ConnectionTimeout.toString,
        IdleTimeout.toString,
      )

  private[daml] def create(x: Map[String, String])(implicit
      readCtx: DBConfig.JdbcConfigDefaults
  ): Either[String, JdbcConfig] =
    create.eval(x)

  override def create(implicit
      readCtx: DBConfig.JdbcConfigDefaults
  ): Fields[JdbcConfig] = for {
    driver <-
      readCtx.defaultDriver.cata(
        dd => optionalStringField("driver").map(_ getOrElse dd),
        requiredField("driver"),
      )
    _ <- StateT liftM Either.cond(
      readCtx.supportedJdbcDrivers(driver),
      (),
      s"$driver unsupported.  Supported drivers: ${readCtx.supportedJdbcDrivers.mkString(", ")}",
    )
    url <- requiredField("url")
    user <- requiredField("user")
    password <- requiredField("password")
    tablePrefix <- optionalStringField("tablePrefix").map(_ getOrElse "")
    maxPoolSize <- optionalIntField("poolSize").map(_ getOrElse PoolSize.Production)
    minIdle <- optionalIntField("minIdle").map(_ getOrElse MinIdle)
    connTimeout <- optionalLongField("connectionTimeout")
      .map(x => x.map(_.millis) getOrElse ConnectionTimeout)
    idleTimeout <- optionalLongField("idleTimeout")
      .map(x => x.map(_.millis) getOrElse IdleTimeout)
  } yield JdbcConfig(
    driver = driver,
    url = url,
    user = user,
    password = password,
    tablePrefix = tablePrefix,
    poolSize = maxPoolSize,
    minIdle = minIdle,
    connectionTimeout = connTimeout,
    idleTimeout = idleTimeout,
  )

  private def helpString(
      driver: String,
      url: String,
      user: String,
      password: String,
      tablePrefix: String,
      poolSize: String,
      minIdle: String,
      connectionTimeout: String,
      idleTimeout: String,
  ): String =
    s"""\"driver=$driver,url=$url,user=$user,password=$password,tablePrefix=$tablePrefix,poolSize=$poolSize,
       |minIdle=$minIdle, connectionTimeout=$connectionTimeout,idleTimeout=$idleTimeout\"""".stripMargin
}
