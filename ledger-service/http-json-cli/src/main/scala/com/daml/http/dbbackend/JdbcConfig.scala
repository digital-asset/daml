// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.typesafe.scalalogging.StrictLogging
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.traverse._
import scalaz.{Show, StateT}

import com.daml.dbutils, dbutils.DBConfig

private[http] final case class JdbcConfig(
    baseConfig: dbutils.JdbcConfig,
    startMode: DbStartupMode = DbStartupMode.StartOnly,
    backendSpecificConf: Map[String, String] = Map.empty,
)

private[http] object JdbcConfig
    extends dbutils.ConfigCompanion[JdbcConfig, DBConfig.JdbcConfigDefaults]("JdbcConfig")
    with StrictLogging {

  implicit val showInstance: Show[JdbcConfig] = Show.shows { a =>
    import a._, baseConfig._
    s"JdbcConfig(driver=$driver, url=$url, user=$user, start-mode=$startMode)"
  }

  private[this] val DisableContractPayloadIndexing = "disableContractPayloadIndexing"

  def help(implicit jcd: DBConfig.JdbcConfigDefaults): String =
    dbutils.JdbcConfig.help(otherOptions =
      s"${indent}createSchema -- boolean flag, if set to true, the process will re-create database schema and terminate immediately. This is deprecated and replaced by start-mode, however if set it will always overrule start-mode.\n" +
        s"${indent}start-mode -- option setting how the schema should be handled. Valid options are ${DbStartupMode.allConfigValues
            .mkString(",")}.\n" +
        (if (jcd.supportedJdbcDrivers exists (_ contains "oracle"))
           s"${indent}$DisableContractPayloadIndexing -- if true, use a slower schema on Oracle that " +
             "supports querying with literals >256 bytes (DRG-50943)\n"
         else "")
    )

  lazy val usage: String = helpString(
    "<JDBC driver class name>",
    "<JDBC connection url>",
    "<user>",
    "<password>",
    "<tablePrefix>",
    s"<${DbStartupMode.allConfigValues.mkString("|")}>",
  )

  override def create(implicit
      readCtx: DBConfig.JdbcConfigDefaults
  ): Fields[JdbcConfig] =
    for {
      baseConfig <- dbutils.JdbcConfig.create
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
      baseConfig = baseConfig,
      startMode = createSchema orElse dbStartupMode getOrElse DbStartupMode.StartOnly,
      backendSpecificConf = remainingConf,
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
