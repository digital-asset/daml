// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.resources.{ResourceContext, TestResourceContext}
import com.daml.logging.LoggingContext
import com.daml.platform.store.FlywayMigrationsBase.{
  MigrateOnEmptySchema,
  MigrationIncomplete,
  NeedsBaselineReset,
  SchemaOutdated,
}
import com.daml.platform.store.backend.VerifiedDataSource
import com.daml.testing.postgresql.PostgresAroundEach
import com.daml.timer.RetryStrategy.TooManyAttemptsException
import org.flywaydb.core.api.FlywayException
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.sql.DataSource
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class FlywayMigrationsBaseForTesting(jdbcUrl: String, baseline: String, folder: String)(implicit
    resourceContext: ResourceContext,
    loggingContext: LoggingContext,
) extends FlywayMigrationsBase {
  override protected def baselineVersion: String = baseline

  override protected def getDataSource: Future[DataSource] =
    VerifiedDataSource(jdbcUrl)(resourceContext.executionContext, loggingContext)

  override protected def locations: List[String] = List(s"classpath:db/migration/test/$folder")
}

class FlywayMigrationsBaseSpec
    extends AsyncFlatSpec
    with Matchers
    with TestResourceContext
    with PostgresAroundEach {

  implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  // Deploying an application to an empty database
  behavior of "FlywayMigrationsBase (database empty, application at v1..v3)"

  it should "not find any version" in {
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      version <- m1_3.currentVersion()
    } yield {
      version shouldBe empty
    }
  }

  it should "migrate v1..v3" in {
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      _ <- m1_3.migrate()
    } yield {
      succeed
    }
  }

  it should "migrateOnEmptySchema v1..v3" in {
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      _ <- m1_3.migrateOnEmptySchema()
    } yield {
      succeed
    }
  }

  it should "fail to validate v1..v3" in {
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      _ <- recoverToSucceededIf[FlywayException](m1_3.validate())
    } yield {
      succeed
    }
  }

  it should "fail to validateAndWaitOnly v1..v3" in {
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      err <- recoverToExceptionIf[TooManyAttemptsException](m1_3.validateAndWaitOnly(1, 1.seconds))
    } yield {
      err.getCause shouldBe a[MigrationIncomplete]
    }
  }

  // Deploying a new application version to an existing database, application contains a new migration
  behavior of "FlywayMigrationsBase (database at v1..v2, application at v1..v3)"

  it should "migrate v1..v2, then currentVersion" in {
    val m1_2 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v2")
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      _ <- m1_2.migrate()
      version1_2 <- m1_2.currentVersion()
      version1_3 <- m1_3.currentVersion()
    } yield {
      version1_2 shouldBe Some("2")
      version1_3 shouldBe Some("2")
    }
  }

  it should "migrate v1..v2, then migrate to v1..v3" in {
    val m1_2 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v2")
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      _ <- m1_2.migrate()
      _ <- m1_3.migrate()
    } yield {
      succeed
    }
  }

  it should "migrate v1..v2, then fail to migrateOnEmptySchema v1..v3" in {
    val m1_2 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v2")
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      _ <- m1_2.migrate()
      _ <- recoverToSucceededIf[MigrateOnEmptySchema](m1_3.migrateOnEmptySchema())
    } yield {
      succeed
    }
  }

  it should "migrate v1..v2, then fail to validate v1..v3" in {
    val m1_2 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v2")
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      _ <- m1_2.migrate()
      _ <- recoverToSucceededIf[FlywayException](m1_3.validate())
    } yield {
      succeed
    }
  }

  it should "migrate v1..v2, then fail to validateAndWaitOnly v1..v3" in {
    val m1_2 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v2")
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")

    for {
      _ <- m1_2.migrate()
      err <- recoverToExceptionIf[TooManyAttemptsException](m1_3.validateAndWaitOnly(1, 1.seconds))
    } yield {
      err.getCause shouldBe a[MigrationIncomplete]
    }
  }

  // Deploying a new application version to an existing database, application squashed old migrations
  behavior of "FlywayMigrationsBase (database at v1..v3, application at v2..v3)"

  it should "migrate to v1..v3, then migrate to v2..v3" in {
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")
    val m2_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "2", "v2_v3")

    for {
      _ <- m1_3.migrate()
      _ <- m2_3.migrate()
    } yield {
      succeed
    }
  }

  it should "migrate v1..v3, then fail to validate v2..v3" in {
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")
    val m2_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "2", "v2_v3")

    for {
      _ <- m1_3.migrate()
      _ <- recoverToSucceededIf[NeedsBaselineReset](m2_3.validate())
    } yield {
      succeed
    }
  }

  it should "migrate v1..v3, then fail to migrateOnEmptySchema v2..v3" in {
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")
    val m2_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "2", "v2_v3")

    for {
      _ <- m1_3.migrate()
      _ <- recoverToSucceededIf[NeedsBaselineReset](m2_3.migrateOnEmptySchema())
    } yield {
      succeed
    }
  }

  it should "migrate v1..v3, then fail to validateAndWaitOnly v2..v3" in {
    val m1_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1_v3")
    val m2_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "2", "v2_v3")

    for {
      _ <- m1_3.migrate()
      err <- recoverToExceptionIf[TooManyAttemptsException](m2_3.validateAndWaitOnly(1, 1.seconds))
    } yield {
      err.getCause shouldBe a[NeedsBaselineReset]
    }
  }

  // Deploying a new application version to an ancient database,
  // migration is impossible because required migrations have been squashed
  behavior of "FlywayMigrationsBase (database at v1, application at v2..v3)"

  it should "migrate to v1, then migrate to v2..v3" in {
    val m1 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1")
    val m2_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "2", "v2_v3")

    for {
      _ <- m1.migrate()
      _ <- recoverToSucceededIf[SchemaOutdated](m2_3.migrate())
    } yield {
      succeed
    }
  }

  it should "migrate v1, then fail to validate v2..v3" in {
    val m1 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1")
    val m2_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "2", "v2_v3")

    for {
      _ <- m1.migrate()
      _ <- recoverToSucceededIf[SchemaOutdated](m2_3.validate())
    } yield {
      succeed
    }
  }

  it should "migrate v1, then fail to migrateOnEmptySchema v2..v3" in {
    val m1 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1")
    val m2_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "2", "v2_v3")

    for {
      _ <- m1.migrate()
      _ <- recoverToSucceededIf[SchemaOutdated](m2_3.migrateOnEmptySchema())
    } yield {
      succeed
    }
  }

  it should "migrate v1, then fail to validateAndWaitOnly v2..v3" in {
    val m1 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "0", "v1")
    val m2_3 = new FlywayMigrationsBaseForTesting(postgresDatabase.url, "2", "v2_v3")

    for {
      _ <- m1.migrate()
      err <- recoverToExceptionIf[TooManyAttemptsException](m2_3.validateAndWaitOnly(1, 1.seconds))
    } yield {
      err.getCause shouldBe a[SchemaOutdated]
    }
  }
}
