// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.operations

import better.files.File
import com.daml.test.evidence.scalatest.OperabilityTestHelpers
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.config.{CantonConfig, DbConfig, IdentityConfig}
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.resource.DbMigrations
import monocle.macros.syntax.lens.*
import org.slf4j.event

import java.nio.file.Files
import scala.util.Random

sealed trait MigrateAndStartIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OperabilityTestHelpers {

  private val dbPrefix = Random.alphanumeric.map(_.toLower).take(7).mkString
  private val remedy = operabilityTest("Nodes")("Database schema version")

  private val migrationPath: File = File.newTemporaryDirectory("migrate-and-start-test")

  private val target = {
    val source = new java.io.File(
      "community/common/src/main/resources/db/migration/canton/postgres/stable/V1_1__initial.sql"
    )
    val tmp = new java.io.File(migrationPath.path.toFile, "V1_1__initial.sql")
    Files.copy(source.toPath, tmp.toPath)
    tmp.deleteOnExit()
    migrationPath.deleteOnExit()
    tmp
  }

  private val externalRepeatableMigrations =
    "filesystem:community/common/src/test/resources/test_table_settings"

  registerPlugin(new UsePostgres(loggerFactory, customDbNames = Some((adjustDbNames, ""))))
  registerPlugin(
    new EnvironmentSetupPlugin {
      override protected def loggerFactory: NamedLoggerFactory =
        MigrateAndStartIntegrationTest.this.loggerFactory

      override def beforeEnvironmentCreated(
          config: CantonConfig
      ): CantonConfig =
        ConfigTransforms
          .updateParticipantConfig("participant3") { config =>
            modifyPgConfig(config, _.focus(_.parameters.migrateAndStart).replace(true))
          }
          .andThen(ConfigTransforms.updateParticipantConfig("participant1") { config =>
            // set participant1 to only 2.0 migration
            modifyPgConfig(
              config,
              _.focus(_.parameters.migrationsPaths)
                .replace(
                  Seq(
                    "filesystem:" + migrationPath.path.toAbsolutePath.toString
                  )
                ),
            )
          })
          .andThen(ConfigTransforms.updateParticipantConfig("participant4") { config =>
            // participant4 gets a repeatable migration
            modifyPgConfig(
              config,
              _.focus(_.parameters.repeatableMigrationsPaths)
                .replace(
                  Seq(
                    externalRepeatableMigrations
                  )
                ),
            )
          })
          .andThen(ConfigTransforms.updateParticipantConfig("participant5") { config =>
            // participant5 gets a misconfigured with a schema migration under repeatable migrations path
            modifyPgConfig(
              config,
              _.focus(_.parameters.repeatableMigrationsPaths)
                .replace(
                  Seq(
                    externalRepeatableMigrations, // note: we keep the path from p4, since Flyway complains if you are removing the applied migrations
                    "filesystem:" + migrationPath.path.toAbsolutePath.toString,
                  )
                ),
            )
          })(config)

      override def afterTests(): Unit = {
        target.delete()
        migrationPath.delete()
      }
    }
  )

  // map all praticipants to the same db
  protected def adjustDbNames(name: String): String =
    if (name.startsWith("participant")) {
      dbPrefix + "_participant"
    } else dbPrefix + "_" + name

  private def modifyPgConfig(
      config: ParticipantNodeConfig,
      modify: DbConfig.Postgres => DbConfig.Postgres,
  ): ParticipantNodeConfig = config.storage match {
    case storage: DbConfig.Postgres =>
      config.focus(_.storage).replace(modify(storage))
    case _ => config
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P5S4M4_Manual
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.init.identity).replace(IdentityConfig.Manual)
        )
      )
      .withSetup { implicit env =>
        new NetworkBootstrapper(EnvironmentDefinition.S1M1).bootstrap()
      }

  "On an empty database" when_ { setting =>
    s"The node is starting up for the first time" must_ { cause =>
      remedy(setting)(cause)("Can be manually initialised") in { implicit env =>
        import env.*
        participant1.db.migrate()
      }
    }
  }
  "On a database with an older schema" when_ { setting =>
    s"A node starting up" must_ { cause =>
      remedy(setting)(cause)("Abort startup and inform admin") in { implicit env =>
        import env.*
        assertThrowsAndLogsCommandFailures(
          participant2.start(),
          _.errorMessage should include("pending migrations to get to database schema"),
        )
      }
    }

    s"A node configured to migrate and start" must_ { cause =>
      remedy(setting)(cause)("Succeed with starting up") in { implicit env =>
        import env.*
        clue("starting p3") {
          participant3.start()
        }
        participant3.stop()
      }
    }

    s"The node with existing database with newly set repeatable migrations" must_ { cause =>
      remedy(setting)(cause)("Succeed with starting up") in { implicit env =>
        import env.*
        clue("starting p4") {
          loggerFactory.assertLogsSeq(
            SuppressionRule.Level(event.Level.INFO) && SuppressionRule.forLogger[DbMigrations]
          )(
            participant4.start(),
            forAtLeast(1, _) {
              _.message should include("Applying 1 new or updated repeatable migrations")
            },
          )
        }
        participant4.stop()
      }
      remedy(setting)(cause)("Not apply repeatable migration again") in { implicit env =>
        import env.*
        clue("force migrating p4 again") {
          loggerFactory.assertLogsSeq(
            SuppressionRule.Level(event.Level.INFO) && SuppressionRule.forLogger[DbMigrations]
          )(
            participant4.db.migrate(),
            forAtLeast(1, _) {
              _.message should include("Applied 0 migrations successfully")
            },
          )
        }
      }
      remedy(setting)(cause)("Apply a modified repeatable migration again") in { implicit env =>
        import env.*

        val file =
          externalRepeatableMigrations.stripPrefix("filesystem:") + "/R__table_settings_tuning.sql"
        // Add a comment to change the checksum
        better.files.File(file).appendLine("-- updated repeatable migration")

        clue("force migrating p4 again 2") {
          loggerFactory.assertLogsSeq(
            SuppressionRule.Level(event.Level.INFO) && SuppressionRule.forLogger[DbMigrations]
          )(
            participant4.db.migrate(),
            forAtLeast(1, _) {
              _.message should include("Applied 1 migrations successfully")
            },
          )
        }
      }
      remedy(setting)(cause)(
        "Fail on V__ schema migration file found under repeatable migrations path"
      ) in { implicit env =>
        import env.*
        clue("run migrations for p5") {
          assertThrowsAndLogsCommandFailures(
            participant5.db.migrate(),
            _.errorMessage should (include("Invalid migration file") and include(
              "under repeatable migrations path"
            )),
          )
        }
      }
    }
  }
}

final class MigrateAndStartReferenceIntegrationTest extends MigrateAndStartIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[Postgres](loggerFactory))
}
