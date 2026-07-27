// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.config
import com.digitalasset.canton.config.{CantonConfig, DbConfig, StorageConfig}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.store.db.DbStorageSetup.DbBasicConfig
import com.typesafe.config.Config
import monocle.Lens
import monocle.macros.GenLens
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

trait DbConfigIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  protected def basicConfig: DbBasicConfig
  protected def mkConfig(bc: DbBasicConfig): Config

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1S1M1_Manual.withManualStart

  private val newConnectionTimeout = config.NonNegativeFiniteDuration.ofMillis(250)

  private val setStorageConnectionTimeout: StorageConfig => StorageConfig = {
    case memConfig: StorageConfig.Memory => memConfig
    case pgConfig: DbConfig.Postgres =>
      pgConfig.focus(_.parameters.connectionTimeout).replace(newConnectionTimeout)
    case h2Config: DbConfig.H2 =>
      h2Config.focus(_.parameters.connectionTimeout).replace(newConnectionTimeout)
  }

  private val modifyStorageConfig: Config => StorageConfig => StorageConfig =
    newConfig => {
      case memConfig: StorageConfig.Memory => memConfig.copy(config = newConfig)
      case pgConfig: DbConfig.Postgres => pgConfig.copy(config = newConfig)
      case h2Config: DbConfig.H2 => h2Config.copy(config = newConfig)
    }

  private val storageConfigLens: Lens[StorageConfig, Config] =
    Lens[StorageConfig, Config](_.config)(modifyStorageConfig)

  private def withPort(port: Int): Config = mkConfig(basicConfig.copy(port = port))
  private def withUser(user: String): Config = mkConfig(basicConfig.copy(username = user))
  private def withPassword(password: String): Config = mkConfig(
    basicConfig.copy(password = password)
  )
  private def withDbName(dbName: String): Config = mkConfig(basicConfig.copy(dbName = dbName))

  private def testWithStorageConfigModifier(
      envConfig: CantonConfig,
      modifier: => Config,
      expectedWarns: Seq[String],
  ): Assertion = {

    // Create a new configuration with storage config modifier applied on participant1
    implicit val newEnv: TestConsoleEnvironment =
      manualCreateEnvironment(
        configTransform = _ =>
          ConfigTransforms.updateParticipantConfig("participant1") { config =>
            val storageLens = GenLens[ParticipantNodeConfig](_.storage)

            storageLens
              .modify(setStorageConnectionTimeout)
              .andThen(storageLens.andThen(storageConfigLens).replace(modifier))(config)
          }(envConfig),
        runPlugins = _ => false,
      )

    import newEnv.*

    loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
      participant1.start(),
      logs =>
        forEvery(logs)(log =>
          assert(
            expectedWarns.exists(log.toString.contains(_)),
            s"line $log contained unexpected problem",
          )
        ),
    )

    newEnv.close()

    succeed
  }

  "fail participant node startup on wrong database port" in { oldEnv =>
    testWithStorageConfigModifier(
      oldEnv.actualConfig,
      withPort(1234),
      Seq(
        "Check that the hostname and port are correct",
        "The Network Adapter could not establish the connection",
      ),
    )
  }

  "fail participant node startup with wrong database user" in { oldEnv =>
    testWithStorageConfigModifier(
      oldEnv.actualConfig,
      withUser("foo"),
      Seq(
        "role \"foo\" does not exist",
        "password authentication failed for user \"foo\"",
        "ORA-01017: invalid username/password; logon denied",
      ),
    )
  }

  "fail participant node startup with wrong database password" in { oldEnv =>
    testWithStorageConfigModifier(
      oldEnv.actualConfig,
      withPassword("foo"),
      Seq(
        "password authentication failed for user \"test\"",
        "ORA-01017: invalid username/password; logon denied",
      ),
    )
  }

  "fail participant node startup with wrong database name" in { oldEnv =>
    testWithStorageConfigModifier(
      oldEnv.actualConfig,
      withDbName("foo"),
      Seq(
        "database \"foo\" does not exist",
        "ORA-12514, TNS:listener does not currently know of service requested in connect descriptor",
      ),
    )
  }
}

class DbConfigIntegrationTestPostgres extends DbConfigIntegrationTest {
  private val postgres = new UsePostgres(loggerFactory)

  registerPlugin(postgres)

  override def basicConfig: DbBasicConfig = postgres.dbSetup.basicConfig
  override def mkConfig(bc: DbBasicConfig): Config = bc.toPostgresConfig
}
