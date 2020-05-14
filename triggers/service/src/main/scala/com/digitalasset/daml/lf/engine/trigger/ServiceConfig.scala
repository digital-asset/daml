// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

//import java.io.File
import java.nio.file.{Path, Paths}
import java.time.Duration
//import com.daml.util.ExceptionOps._
import com.daml.platform.services.time.TimeProviderType
//import scalaz.{Show, \/}
//import scalaz.std.option._
//import scalaz.syntax.traverse._
//import scala.util.Try

case class ServiceConfig(
    // For convenience, we allow passing in a DAR on startup
    // as opposed to uploading it dynamically.
    darPath: Option[Path],
    httpPort: Int,
    ledgerHost: String,
    ledgerPort: Int,
    timeProviderType: TimeProviderType,
    commandTtl: Duration,
//    init: Boolean,
//    jdbcConfig: Option[JdbcConfig] = None,
//    initConfig: InitConfig,
)

//final case class JdbcConfig(
//    driver: String,
//    url: String,
//    user: String,
//    password: String,
//    createSchema: Boolean = false
//)
//case class InitConfig(
//    operatorUsername: String,
//    operatorPassword: String,
//    serviceUsername: String,
//    servicePassword: String,
//    databaseName: String,
//)

//private abstract class ConfigCompanion[A](name: String) {
//
//  protected val indent: String = List.fill(8)(" ").mkString
//
//  def create(x: Map[String, String]): Either[String, A]
//
//  def createUnsafe(x: Map[String, String]): A = create(x).fold(
//    e => sys.error(e),
//    identity
//  )
//
//  def validate(x: Map[String, String]): Either[String, Unit] =
//    create(x).map(_ => ())
//
//  protected def requiredField(m: Map[String, String])(k: String): Either[String, String] =
//    m.get(k).filter(_.nonEmpty).toRight(s"Invalid $name, must contain '$k' field")
//
//  @SuppressWarnings(Array("org.wartremover.warts.Any"))
//  protected def optionalBooleanField(m: Map[String, String])(
//    k: String): Either[String, Option[Boolean]] =
//    m.get(k).traverse(v => parseBoolean(k)(v)).toEither
//
//  @SuppressWarnings(Array("org.wartremover.warts.Any"))
//  protected def optionalLongField(m: Map[String, String])(k: String): Either[String, Option[Long]] =
//    m.get(k).traverse(v => parseLong(k)(v)).toEither
//
//  import scalaz.syntax.std.string._
//
//  protected def parseBoolean(k: String)(v: String): String \/ Boolean =
//    v.parseBoolean.leftMap(e => s"$k=$v must be a boolean value: ${e.description}").disjunction
//
//  protected def parseLong(k: String)(v: String): String \/ Long =
//    v.parseLong.leftMap(e => s"$k=$v must be a int value: ${e.description}").disjunction
//
//  protected def requiredDirectoryField(m: Map[String, String])(k: String): Either[String, File] =
//    requiredField(m)(k).flatMap(directory)
//
//  protected def directory(s: String): Either[String, File] =
//    Try(new File(s).getAbsoluteFile).toEither.left
//      .map(e => e.description)
//      .flatMap { d =>
//        if (d.isDirectory) Right(d)
//        else Left(s"Directory does not exist: ${d.getAbsolutePath}")
//      }
//}
//
//private object JdbcConfig extends ConfigCompanion[JdbcConfig]("JdbcConfig") {
//
//  implicit val showInstance: Show[JdbcConfig] = Show.shows(a =>
//    s"JdbcConfig(driver=${a.driver}, url=${a.url}, user=${a.user}, createSchema=${a.createSchema})")
//
//  lazy val help: String =
//    "Contains comma-separated key-value pairs. Where:\n" +
//      s"${indent}driver -- JDBC driver class name, only org.postgresql.Driver supported right now,\n" +
//      s"${indent}url -- JDBC connection URL, only jdbc:postgresql supported right now,\n" +
//      s"${indent}user -- database user name,\n" +
//      s"${indent}password -- database user password,\n" +
//      s"${indent}createSchema -- boolean flag, if set to true, the process will re-create database schema and terminate immediately.\n" +
//      s"${indent}Example: " + helpString(
//      "org.postgresql.Driver",
//      "jdbc:postgresql://localhost:5432/test?&ssl=true",
//      "postgres",
//      "password",
//      "false")
//
//  lazy val usage: String = helpString(
//    "<JDBC driver class name>",
//    "<JDBC connection url>",
//    "<user>",
//    "<password>",
//    "<true|false>")
//
//  override def create(x: Map[String, String]): Either[String, JdbcConfig] =
//    for {
//      driver <- requiredField(x)("driver")
//      url <- requiredField(x)("url")
//      user <- requiredField(x)("user")
//      password <- requiredField(x)("password")
//      createSchema <- optionalBooleanField(x)("createSchema")
//    } yield
//      JdbcConfig(
//        driver = driver,
//        url = url,
//        user = user,
//        password = password,
//        createSchema = createSchema.getOrElse(false)
//      )
//
//  private def helpString(
//                          driver: String,
//                          url: String,
//                          user: String,
//                          password: String,
//                          createSchema: String): String =
//    s"""\"driver=$driver,url=$url,user=$user,password=$password,createSchema=$createSchema\""""
//}

object ServiceConfig {
  private val parser = new scopt.OptionParser[ServiceConfig]("trigger-service") {
    head("trigger-service")

    opt[String]("dar")
      .optional()
      .action((f, c) => c.copy(darPath = Some(Paths.get(f))))
      .text("Path to the dar file containing the trigger")

    opt[Int]("http-port")
      .optional()
      .action((t, c) => c.copy(httpPort = t))
      .text("Http port")

    opt[String]("ledger-host")
      .required()
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .required()
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port")

    opt[Unit]('w', "wall-clock-time")
      .action { (t, c) =>
        c.copy(timeProviderType = TimeProviderType.WallClock)
      }
      .text("Use wall clock time (UTC). When not provided, static time is used.")

    opt[Long]("ttl")
      .action { (t, c) =>
        c.copy(commandTtl = Duration.ofSeconds(t))
      }
      .text("TTL in seconds used for commands emitted by the trigger. Defaults to 30s.")

//    opt[JdbcConfig]("jdbcConfig")
//        .action((t, c) => c.copy(jdbcConfig = t))
//        .text("JDBC config string.")
//
//    cmd("init")
//      .action((_, c) => c.copy(init = true))
//      .text("Initialize a PostgreSQL database for service recovery.")
  }
  def parse(args: Array[String]): Option[ServiceConfig] =
    parser.parse(
      args,
      ServiceConfig(
        darPath = None,
        httpPort = 8088,
        ledgerHost = null,
        ledgerPort = 0,
        timeProviderType = TimeProviderType.Static,
        commandTtl = Duration.ofSeconds(30L),
//        init = false
      ),
    )
}
