// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.operations

import better.files.*
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.{CantonConfig, DbConfig, ModifiableDbConfig, StorageConfig}
import com.digitalasset.canton.console.{CommandFailure, LocalInstanceReference}
import com.digitalasset.canton.integration.plugins.{UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  IsolatedEnvironments,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.resource.DbStorageSingle
import com.digitalasset.canton.util.ResourceUtil
import monocle.macros.syntax.lens.*

/** Verifies that canton behaves correctly when a new sql migration is added after the migrations
  * have been previously run. We test this by copying all of the usual migrations to a temporary
  * directory, starting and stopping canton once so the original set of migrations will be applied,
  * then add a new migration file to this temporary directory and see if canton prevents a startup
  * until it has been applied. We use a temporary directory so not to confuse any instances of
  * canton concurrently running when the new migration file is added.
  */
trait FlywayMigrationIntegrationTest
    extends CommunityIntegrationTest
    with IsolatedEnvironments
    with FlagCloseable
    with HasExecutionContext {

  private val customMigrationsPlugin =
    new UseCustomMigrationsPath(loggerFactory, testedProtocolVersion.isDev)

  protected def setupPlugins(storagePlugin: EnvironmentSetupPlugin): Unit = {
    registerPlugin(storagePlugin)

    registerPlugin(customMigrationsPlugin)
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual

  private def tryCreateStorage(
      localInstance: LocalInstanceReference
  )(implicit env: TestConsoleEnvironment, closeContext: CloseContext): DbStorageSingle =
    DbStorageSingle.tryCreate(
      config = localInstance.config.storage.asInstanceOf[DbConfig],
      clock = env.environment.clock,
      scheduler = None,
      connectionPoolForParticipant = false,
      logQueryCost = None,
      metrics = CommonMockMetrics.dbStorage,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )

  private def getVersions(
      localInstance: LocalInstanceReference
  )(implicit env: TestConsoleEnvironment, closeContext: CloseContext): Seq[String] =
    ResourceUtil.withResource(tryCreateStorage(localInstance)) { storage =>
      import storage.api.*

      storage
        .query(
          sql"""SELECT "version" FROM "flyway_schema_history" """.as[String],
          "select",
        )
        .futureValueUS
        .filter(_ != null)
    }

  private def stopNodes()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    participant1.stop()
    participant2.stop()
    mediator1.stop()
    sequencer1.stop()
  }

  private def startNodes()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    sequencer1.start()
    mediator1.start()
    participant1.start()
    participant2.start()
  }

  private def applyMigrations()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*
    participant1.db.migrate()
    participant2.db.migrate()
    mediator1.db.migrate()
    sequencer1.db.migrate()
  }

  "require to run migration command if there is new pending migration" in { implicit env =>
    import env.*

    // when starting for the first time, migrations will run automatically
    startNodes()
    Threading.sleep(5000)

    stopNodes()

    // adding a new dummy migration file
    (customMigrationsPlugin.migrationPath / "V1000__Create_table.sql")
      .write("create table foo (id integer primary key);")

    // will fail to start this time because there is one pending migration that needs to be applied manually
    loggerFactory.suppressWarningsErrorsExceptions[CommandFailure] {
      participant1.start()
    }
    loggerFactory.suppressWarningsErrorsExceptions[CommandFailure] {
      participant2.start()
    }
    loggerFactory.suppressWarningsErrorsExceptions[CommandFailure] {
      mediator1.start()
    }
    loggerFactory.suppressWarningsErrorsExceptions[CommandFailure] {
      sequencer1.start()
    }

    // apply pending migration
    applyMigrations()

    // after running the migrations, nodes can be started normally
    startNodes()

  }

  "repair migration fails if force isn't explicitly used" in { implicit env =>
    import env.*

    loggerFactory.assertThrowsAndLogs[CommandFailure](
      participant1.db.repair_migration(),
      logEntry =>
        logEntry.errorMessage should include(
          "call `participant1.db.repair_migration(force=true)`"
        ),
    )

    participant1.db.repair_migration(force = true)
  }

  "migration command fails if an existing migration file is modified but succeeds once the migration files are repaired" in {
    implicit env =>
      import env.*

      (customMigrationsPlugin.migrationPath / "V1001__Create_table.sql")
        .write("create table foobar (id integer primary key);")

      participant1.db.migrate()

      (customMigrationsPlugin.migrationPath / "V1001__Create_table.sql")
        .write("create table barfoor (id_x integer primary key);")

      loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
        participant1.db.migrate(),
        logEntry =>
          logEntry.warningMessage should include(
            "Migration checksum mismatch for migration version 1001"
          ),
        logEntry => logEntry.errorMessage should include("FlywayError"),
      )
      loggerFactory.suppressWarningsErrorsExceptions[Throwable] {
        participant1.db.migrate()
      }

      participant1.db.repair_migration(force = true)
      participant1.db.migrate()
      participant1.start()
  }

  "previously applied migrations can be dropped" in { implicit env =>
    import env.*

    implicit val closeContext = CloseContext(this)

    // when starting for the first time, migrations will run automatically
    startNodes()

    Threading.sleep(5000)

    // adding a new dummy migration file
    (customMigrationsPlugin.migrationPath / "V2000__Create_table.sql")
      .write("ALTER TABLE common_node_id ADD test_column_deux INT DEFAULT 0 NOT NULL;")

    stopNodes()
    applyMigrations()
    startNodes()

    (customMigrationsPlugin.migrationPath / "V1500__Create_table.sql")
      .write("create table foo1500 (id integer primary key);")

    stopNodes()

    // Cannot be applied because version is smaller than the previously applied migration
    loggerFactory.assertThrowsAndLogsUnordered[CommandFailure](
      applyMigrations(),
      logEntry =>
        logEntry.warningMessage should include(
          "Detected resolved migration not applied to database: 1500."
        ),
      logEntry => logEntry.errorMessage should include("FlywayError"),
    )

    val versionsBeforeRemoval = getVersions(participant1)
    versionsBeforeRemoval should not contain "1500"
    versionsBeforeRemoval should contain("2000")

    // Manually removing migration 1000 from Flyway history
    Seq[LocalInstanceReference](participant1, participant2, mediator1, sequencer1).foreach {
      localInstance =>
        ResourceUtil.withResource(tryCreateStorage(localInstance)) { storage =>
          import storage.api.*

          storage
            .update_(
              sqlu"""DELETE FROM "flyway_schema_history" WHERE "version" = '2000' """,
              "select",
            )
            .futureValueUS
        }
    }

    // Undoing the migration
    Seq[LocalInstanceReference](participant1, participant2, mediator1, sequencer1).foreach {
      localInstance =>
        ResourceUtil.withResource(tryCreateStorage(localInstance)) { storage =>
          import storage.api.*

          storage
            .update_(
              sqlu"""ALTER TABLE common_node_id DROP COLUMN test_column_deux """,
              "select",
            )
            .futureValueUS
        }
    }

    // Delete the migration file
    // Note, this is equivalent to not including the directory if only non-standard-config (and not
    // enable-dev-support) are activated.
    (customMigrationsPlugin.migrationPath / "V2000__Create_table.sql")
      .delete()

    applyMigrations()

    val versionsAfterRemoval = getVersions(participant1)
    versionsAfterRemoval should contain("1500")
    versionsAfterRemoval should not contain "2000"

    startNodes()
    stopNodes()
  }
}

class UseCustomMigrationsPath(
    protected val loggerFactory: NamedLoggerFactory,
    isDevVersion: Boolean,
) extends EnvironmentSetupPlugin {

  val migrationPath: File = File.newTemporaryDirectory("migrations-test")

  private def copyMigrations(path: String, dest: File): Unit = {
    // our copying assumes we're copying from the classpath
    // double-check that's the case
    require(
      path.startsWith("classpath:"),
      "expecting the default migrations to be stored on the classpath",
    )

    val resourcePath = path.stripPrefix("classpath:")
    val resourceLoader = Resource.from(Thread.currentThread().getContextClassLoader)

    // list all the files in the resource directory (yes, this is apparently how you do this ¯\_(ツ)_/¯)
    resourceLoader
      .getAsString(resourcePath)
      .split(System.lineSeparator())
      .toSeq
      .foreach { filename =>
        if (filename.nonEmpty) {
          // then copy each file to our fake migrations directory
          val content = resourceLoader.getAsString(s"$resourcePath/$filename")
          (dest / filename).write(content)
        }
      }
  }

  private def updateStorage(
      update: (String, StorageConfig) => StorageConfig
  ): ConfigTransform =
    ConfigTransforms.updateAllParticipantConfigs { case (nodeName, config) =>
      config.focus(_.storage).modify(update(nodeName, _))
    } compose ConfigTransforms.updateAllSequencerConfigs { case (nodeName, config) =>
      config.focus(_.storage).modify(update(nodeName, _))
    } compose ConfigTransforms.updateAllMediatorConfigs { case (nodeName, config) =>
      config.focus(_.storage).modify(update(nodeName, _))
    }

  private val useCustomMigrations: (String, StorageConfig) => StorageConfig = {
    case (_, dbConfig: ModifiableDbConfig[?]) =>
      val prevMigrationsPath = dbConfig.buildMigrationsPaths(alphaVersionSupport = isDevVersion)
      // Copy original migrations
      prevMigrationsPath.foreach(prev => copyMigrations(prev, migrationPath))
      dbConfig
        .modify(parameters =
          dbConfig.parameters
            .focus(_.migrationsPaths)
            .replace(
              Seq(s"filesystem:${migrationPath.path.toAbsolutePath}")
            )
        )
    case _ => sys.error("every node should be using h2/postgres")
  }

  override def beforeEnvironmentCreated(config: CantonConfig): CantonConfig =
    updateStorage(useCustomMigrations)(config)

  override def afterTests(): Unit =
    try super.afterTests()
    finally {
      migrationPath.delete()
    }
}

class FlywayMigrationIntegrationTestH2 extends FlywayMigrationIntegrationTest {
  setupPlugins(new UseH2(loggerFactory))
}

class FlywayMigrationIntegrationTestPostgres extends FlywayMigrationIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))
}
