// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.store.FlywayMigrations._
import com.digitalasset.platform.store.dao.HikariConnection
import com.digitalasset.resources.Resource
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.configuration.FluentConfiguration

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.NonFatal

class FlywayMigrations(jdbcUrl: String)(implicit logCtx: LoggingContext) {
  private val logger = ContextualizedLogger.get(this.getClass)

  private val dbType = DbType.jdbcType(jdbcUrl)

  private def newDataSource()(
      implicit executionContext: ExecutionContext
  ): Resource[HikariDataSource] =
    HikariConnection.owner(jdbcUrl, "daml.index.db.migration", 2, 2, 250.millis, None).acquire()

  def validate(): Unit = {
    val dataSourceResource = newDataSource()(DirectExecutionContext)
    val ds = Await.result(dataSourceResource.asFuture, 1.second)
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
      val _ = Await.result(dataSourceResource.release(), 1.second)
    }
  }

  def migrate(allowExistingSchema: Boolean = false): Unit = {
    val dataSourceResource = newDataSource()(DirectExecutionContext)
    val ds = Await.result(dataSourceResource.asFuture, 1.second)
    try {
      val flyway = configurationBase(dbType)
        .dataSource(ds)
        .baselineOnMigrate(allowExistingSchema)
        .baselineVersion(MigrationVersion.fromVersion("0"))
        .load()
      logger.info(s"running Flyway migration..")
      val stepsTaken = flyway.migrate()
      logger.info(s"Flyway schema migration finished successfully, applying $stepsTaken steps.")
    } catch {
      case NonFatal(e) =>
        logger.error("an error occurred while running schema migration", e)
        //TODO: shall we quit gracefully if something goes off track?
        //there is little point in communicating this error in a typed manner, we should rather blow up
        throw e
    } finally {
      val _ = Await.result(dataSourceResource.release(), 1.second)
    }
  }
}

object FlywayMigrations {
  def configurationBase(dbType: DbType): FluentConfiguration =
    Flyway.configure().locations("classpath:db/migration/" + dbType.name)
}
