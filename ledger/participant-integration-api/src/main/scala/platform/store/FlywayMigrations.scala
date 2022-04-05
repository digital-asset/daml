// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.resources.ResourceContext
import com.daml.logging.LoggingContext
import com.daml.platform.store.backend.VerifiedDataSource
import javax.sql.DataSource
import scala.concurrent.{ExecutionContext, Future}

private[daml] class FlywayMigrations(
    jdbcUrl: String,
    additionalMigrationPaths: Seq[String] = Seq.empty,
)(implicit resourceContext: ResourceContext, loggingContext: LoggingContext)
    extends FlywayMigrationsBase {
  private val dbType = DbType.jdbcType(jdbcUrl)
  implicit private val ec: ExecutionContext = resourceContext.executionContext

  // A "baseline migration" represents all migrations up to and including the "baseline version"
  // Baseline migrations are a first class feature in Flyway, however they are not available in the free version.
  // To move the baseline version, use the following procedure:
  //   1. Apply all migrations up to and including the new baseline version to a fresh database
  //   2. Export the resulting schema as the new baseline migration, and assign it a version number equal to the
  //      baseline version
  //   3. Double check that the migrations from step 1 and the migration from step 2 produce exactly the same
  //      database
  //   4. Delete all migrations used in step 1
  //   5. Update the value below
  override protected def baselineVersion: String = dbType match {
    case DbType.Postgres => "38"
    case DbType.Oracle => "0"
    case DbType.H2Database => "0"
  }

  override protected def getDataSource: Future[DataSource] = VerifiedDataSource(jdbcUrl)

  override protected def locations: List[String] =
    FlywayMigrations.locations(dbType) ++ additionalMigrationPaths
}

private[platform] object FlywayMigrations {
  private val sqlMigrationClasspathBase = "classpath:db/migration/"
  private val javaMigrationClasspathBase = "classpath:com/daml/platform/db/migration/"

  private[platform] def locations(dbType: DbType) =
    // TODO append-only: rename the -appendonly folders
    List(
      sqlMigrationClasspathBase + dbType.name + "-appendonly",
      javaMigrationClasspathBase + dbType.name,
    )
}
