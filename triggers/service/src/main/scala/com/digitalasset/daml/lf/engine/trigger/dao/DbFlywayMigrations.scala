// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger
package dao

import com.typesafe.scalalogging.StrictLogging
import doobie.free.connection.ConnectionIO
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.configuration.FluentConfiguration

import javax.sql.DataSource

private[trigger] class DbFlywayMigrations(private val ds: DataSource, migrationsDir: String)
    extends StrictLogging {
  import DbFlywayMigrations._

  var flyway: Flyway = _

  def migrate(allowExistingSchema: Boolean): ConnectionIO[Unit] = {
    doobie.free.connection.delay {
      flyway = configurationBase(migrationsDir)
        .dataSource(ds)
        .baselineOnMigrate(allowExistingSchema)
        .baselineVersion(MigrationVersion.fromVersion("0"))
        .load()
      logger.info("Running Flyway migration...")
      val stepsTaken = flyway.migrate()
      logger.info(s"Flyway schema migration finished successfully, applying $stepsTaken steps.")
    }
  }

  def clean(): ConnectionIO[Unit] =
    doobie.free.connection.delay {
      logger.info("Running Flyway clean...")
      val stepsTaken = flyway.clean()
      logger.info(s"Flyway clean finished successfully, applying $stepsTaken steps.")
    }
}

private[trigger] object DbFlywayMigrations {
  def configurationBase(migrationsDir: String): FluentConfiguration =
    Flyway
      .configure()
      .locations(
        s"classpath:com/daml/lf/engine/trigger/db/migration/$migrationsDir"
      )
}
