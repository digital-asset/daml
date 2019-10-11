// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.migration

import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{DbType, HikariConnection}
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.configuration.FluentConfiguration

import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

class FlywayMigrations(jdbcUrl: String, loggerFactory: NamedLoggerFactory) {
  import FlywayMigrations._

  private val logger = loggerFactory.getLogger(getClass)

  private val dbType = DbType.jdbcType(jdbcUrl)
  private def newDataSource =
    HikariConnection.createDataSource(jdbcUrl, "Flyway-Pool", 2, 2, 250.millis)

  def validate(): Unit = {
    val ds = newDataSource
    try {
      val flyway = configurationBase(dbType).dataSource(ds).load()
      logger.info(s"running Flyway validation..")
      flyway.validate()
      logger.info(s"Flyway schema validation finished successfully")
    } catch {
      case NonFatal(e) =>
        logger.error("an error occurred while running schema migration", e)
        //there is little point in communicating this error in a typed manner, we should rather blow up
        throw e
    } finally {
      val _ = Try(ds.close())
    }
  }

  def migrate(): Unit = {
    val ds = newDataSource
    try {
      val flyway = configurationBase(dbType).dataSource(ds).load()
      logger.info(s"running Flyway migration..")
      val stepsTaken = flyway.migrate()
      logger.info(s"Flyway schema migration finished successfully applying ${stepsTaken} steps.")
    } catch {
      case NonFatal(e) =>
        logger.error("an error occurred while running schema migration", e)
        //TODO: shall we quit gracefully if something goes off track?
        //there is little point in communicating this error in a typed manner, we should rather blow up
        throw e
    } finally {
      val _ = Try(ds.close())
    }
  }

}

object FlywayMigrations {

  def configurationBase(dbType: DbType): FluentConfiguration =
    Flyway.configure.locations("classpath:db/migration/" + dbType.name)

  def apply(jdbcUrl: String, loggerFactory: NamedLoggerFactory): FlywayMigrations =
    new FlywayMigrations(jdbcUrl, loggerFactory)
}
