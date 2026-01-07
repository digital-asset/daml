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
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import monocle.macros.syntax.lens.*

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
          })(config)

      override def afterTests(): Unit = {
        target.delete()
        migrationPath.delete()
      }
    }
  )

  // map both praticipants to the same db
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
    EnvironmentDefinition.P3_S1M1_Manual
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
      }
    }
  }
}

final class MigrateAndStartReferenceIntegrationTest extends MigrateAndStartIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[Postgres](loggerFactory))
}
