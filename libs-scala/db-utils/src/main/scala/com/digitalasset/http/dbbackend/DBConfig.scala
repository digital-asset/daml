// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.dbutils

import com.typesafe.scalalogging.StrictLogging
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{Show, StateT, \/}

import java.io.File
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
    minIdle: Int = JdbcConfig.MinIdle,
    connectionTimeout: Long = JdbcConfig.ConnectionTimeout,
    idleTimeout: Long = JdbcConfig.IdleTimeout,
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
    extends ConfigCompanion[JdbcConfig, DBConfig.JdbcConfigDefaults]("JdbcConfig")
    with StrictLogging {

  final val MinIdle = 8
  final val IdleTimeout = 10000L // ms, minimum according to log, defaults to 600s
  final val ConnectionTimeout = 5000L

  @scala.deprecated("do I need this?", since = "SC")
  implicit val showInstance: Show[JdbcConfig] =
    Show.shows(a => s"JdbcConfig(driver=${a.driver}, url=${a.url}, user=${a.user})")

  def help(otherOptions: String = "")(implicit jcd: DBConfig.JdbcConfigDefaults): String =
    "Contains comma-separated key-value pairs. Where:\n" +
      s"${indent}driver -- JDBC driver class name, ${jcd.supportedJdbcDrivers.mkString(", ")} supported right now,\n" +
      s"${indent}url -- JDBC connection URL,\n" +
      s"${indent}user -- database user name,\n" +
      s"${indent}password -- database user password,\n" +
      otherOptions +
      s"${indent}Example: " + helpString(
        "org.postgresql.Driver",
        "jdbc:postgresql://localhost:5432/test?&ssl=true",
        "postgres",
        "password",
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
  } yield JdbcConfig(
    driver = driver,
    url = url,
    user = user,
    password = password,
  )

  private def helpString(
      driver: String,
      url: String,
      user: String,
      password: String,
  ): String =
    s"""\"driver=$driver,url=$url,user=$user,password=$password\""""
}
