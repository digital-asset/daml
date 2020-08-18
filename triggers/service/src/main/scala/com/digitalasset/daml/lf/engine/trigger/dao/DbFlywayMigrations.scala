// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package dao

import com.typesafe.scalalogging.StrictLogging
import doobie.free.connection.ConnectionIO
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.configuration.FluentConfiguration

import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}

private[trigger] class DbFlywayMigrations(private val ds: DataSource) extends StrictLogging {
  import DbFlywayMigrations._

  def validate()(implicit executionContext: ExecutionContext): Future[Unit] =
    dataSource.use { ds =>
      Future {
        val flyway = configurationBase().dataSource(ds).load()
        logger.info("Running Flyway validation...")
        flyway.validate()
        logger.info("Flyway schema validation finished successfully.")
      }
    }

  def migrate(allowExistingSchema: Boolean = false): ConnectionIO[Unit] =
    doobie.free.connection.delay {
      val flyway = configurationBase()
        .dataSource(ds)
        .baselineOnMigrate(allowExistingSchema)
        .baselineVersion(MigrationVersion.fromVersion("0"))
        .load()
      logger.info("Running Flyway migration...")
      val stepsTaken = flyway.migrate()
      logger.info(s"Flyway schema migration finished successfully, applying $stepsTaken steps.")
    }

  def reset()(implicit executionContext: ExecutionContext): Future[Unit] =
    dataSource.use { ds =>
      Future {
        val flyway = configurationBase().dataSource(ds).load()
        logger.info("Running Flyway clean...")
        flyway.clean()
        logger.info("Flyway schema clean finished successfully.")
      }
    }

  private object dataSource {
    def use[T](f: ds.type => Future[T]): Future[T] =
      f(ds)
  }
}

private[trigger] object DbFlywayMigrations {
  def configurationBase(): FluentConfiguration =
    Flyway
      .configure()
      .locations(
        "classpath:com/daml/lf/engine/trigger/db/migration/postgres",
      )
}
