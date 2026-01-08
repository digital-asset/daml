// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authenticity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.admin.api.client.commands.VaultAdminCommands
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.config.{AuthServiceConfig, PositiveFiniteDuration}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

trait AdminApiKeyRotationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite {

  private val jwtSecret = NonEmptyString.tryCreate("pyjama")
  private val adminToken = CantonAdminToken.create(new SymbolicPureCrypto())
  private val tokenDurationCfg = PositiveFiniteDuration.ofSeconds(10)
  private val tokenDuration = tokenDurationCfg.asJava
  private val rotationInterval = tokenDuration.dividedBy(2)

  lazy private val securityAsset: SecurityTest =
    SecurityTest(property = Authenticity, asset = "admin api client")

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") { config =>
        config
          .focus(_.adminApi.adminTokenConfig.fixedAdminToken)
          .replace(Some(adminToken.secret))
          .focus(_.adminApi.adminTokenConfig.adminTokenDuration)
          .replace(tokenDurationCfg)
          .focus(_.adminApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = jwtSecret,
                  targetAudience = None,
                  targetScope = None,
                )
            )
          )
          .focus(_.adminApi.adminTokenConfig.adminClaim)
          .replace(true)
      })

  private val grpcAdminCommand = VaultAdminCommands.ListPublicKeys(
    "",
    "",
    Set.empty,
    Set.empty,
  )

  "A participant" should {
    "allow admin command with fixed token and tokens from the dispenser" taggedAs securityAsset
      .setHappyCase(
        "use fixed and dispensed admin tokens"
      ) in { implicit env =>
      import env.*

      participant1.keys.public.list().size should be > 0

      val token0 = participant1.adminToken
      token0 should not be Some(adminToken.secret)

      // sleep to allow token rotation
      Threading.sleep(rotationInterval.toMillis + 1)

      val token1 = participant1.adminToken
      token1 should not be token0
      token1 should not be Some(adminToken.secret)

      // the first token should still be valid
      env.run(
        grpcAdminCommandRunner
          .runCommand(
            participant1.name,
            grpcAdminCommand,
            participant1.config.clientAdminApi,
            token0,
          )
      )

      participant1.keys.public.list().size should be > 0

      // fixed token should always be valid
      env.run(
        grpcAdminCommandRunner
          .runCommand(
            participant1.name,
            grpcAdminCommand,
            participant1.config.clientAdminApi,
            Some(adminToken.secret),
          )
      )
    }

    "not allow admin command with an invalid token" taggedAs securityAsset.setAttack(
      Attack(
        "Admin api client",
        "provides an invalid token",
        "refuse access to the admin service with a failure",
      )
    ) in { implicit env =>
      import env.*

      assertUnauthenticated {
        env.run(
          grpcAdminCommandRunner
            .runCommand(
              participant1.name,
              grpcAdminCommand,
              participant1.config.clientAdminApi,
              Some("invalidToken"),
            )
        )
      }
    }

    "not allow admin command with no token" taggedAs securityAsset.setAttack(
      Attack(
        "Admin api client",
        "does not provide an admin token",
        "refuse access to the admin service with a failure",
      )
    ) in { implicit env =>
      import env.*

      assertUnauthenticated {
        env.run(
          grpcAdminCommandRunner
            .runCommand(
              participant1.name,
              grpcAdminCommand,
              participant1.config.clientAdminApi,
              None,
            )
        )
      }
    }

    "not allow admin command with a dispensed token after it expires" taggedAs securityAsset
      .setAttack(
        Attack(
          "Admin api client",
          "uses a previously valid dispensed token after its validity window",
          "refuse access to the admin service with a failure",
        )
      ) in { implicit env =>
      import env.*

      // get a dispensed token
      val dispensedToken = participant1.adminToken
      dispensedToken should not be Some(adminToken.secret)

      env.run(
        grpcAdminCommandRunner
          .runCommand(
            participant1.name,
            grpcAdminCommand,
            participant1.config.clientAdminApi,
            dispensedToken,
          )
      )

      // wait for the token to expire
      Threading.sleep(tokenDuration.toMillis + 1)

      assertUnauthenticated {
        env.run(
          grpcAdminCommandRunner
            .runCommand(
              participant1.name,
              grpcAdminCommand,
              participant1.config.clientAdminApi,
              dispensedToken,
            )
        )
      }
    }
  }

  def assertUnauthenticated[A](f: => A): Assertion =
    loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
      f,
      entries => {
        assert(entries.nonEmpty)
        forAtLeast(1, entries) { entry =>
          entry.errorMessage should include("UNAUTHENTICATED")
        }
      },
    )

}

// Don't run in-memory because this may fail due to stale reads in H2.
class AdminApiKeyRotationIntegrationTestDefault extends AdminApiKeyRotationIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class AdminApiKeyRotationIntegrationTestPostgres extends AdminApiKeyRotationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
