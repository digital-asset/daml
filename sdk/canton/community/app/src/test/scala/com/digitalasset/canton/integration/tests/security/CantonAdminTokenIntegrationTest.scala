// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.*
import com.digitalasset.canton.auth.CantonAdminToken
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.CantonRequireTypes.{InstanceName, NonEmptyString}
import com.digitalasset.canton.console.CommandFailure
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.participant.config.RemoteParticipantConfig
import com.digitalasset.canton.participant.ledger.api.JwtTokenUtilities
import monocle.macros.syntax.lens.*

/** Test CantonAdminToken
  *
  * Scenario 1:
  *   - participant1 is configured with an adminToken with all permissions.
  *   - remote admin1 uses the correct admin token that was also set-up on the participant.
  *   - remote valid1 uses a correct regular token based on a shared secret.
  *   - remote invalid1 uses the incorrect admin token.
  *   - remote noToken1 uses no token whatsoever.
  */

/** Scenario 2:
  *   - participant1 is configured with an adminToken with few permissions.
  *   - remote admin1 uses the correct admin token that was also set-up on the participant.
  *   - remote valid1 uses a correct regular token based on a shared secret.
  */
class CantonAdminTokenIntegrationTest(elevatedRights: Boolean)
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite
    with AccessTestScenario {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  private val mySecret = NonEmptyString.tryCreate("pyjama")

  private val adminToken = CantonAdminToken.create(new SymbolicPureCrypto())

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      // add adminToken for p1
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") { config =>
        config
          .focus(_.ledgerApi.adminTokenConfig)
          .replace(
            AdminTokenConfig(
              fixedAdminToken = Some(adminToken.secret),
              actAsAnyPartyClaim = elevatedRights,
              adminClaim = elevatedRights,
            )
          )
          .focus(_.ledgerApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = mySecret,
                  targetAudience = None,
                  targetScope = None,
                )
            )
          )
      })
      // remote configurations for participant1
      .addConfigTransform { config =>
        val p1 = config.participantsByString("participant1")
        val noToken1 =
          RemoteParticipantConfig(
            adminApi = p1.adminApi.clientConfig,
            ledgerApi = p1.ledgerApi.clientConfig,
          )
        val admin1 = noToken1.copy(token = Some(adminToken.secret))
        val valid1 = noToken1.copy(token =
          Some(
            JwtTokenUtilities.buildUnsafeToken(
              mySecret.unwrap,
              Some(UserManagementStore.DefaultParticipantAdminUserId),
            )
          )
        )
        val invalid1 = noToken1.copy(token =
          Some(
            JwtTokenUtilities
              .buildUnsafeToken("invalid", Some(UserManagementStore.DefaultParticipantAdminUserId))
          )
        )
        config
          .focus(_.remoteParticipants)
          .replace(
            config.remoteParticipants ++ Map(
              InstanceName.tryCreate("admin1") -> admin1,
              InstanceName.tryCreate("valid1") -> valid1,
              InstanceName.tryCreate("noToken1") -> noToken1,
              InstanceName.tryCreate("invalid1") -> invalid1,
            )
          )
      }
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            // excluding local aliases
            "admin1",
            "valid1",
            "noToken1",
            "invalid1",
          )
        )
      )
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)
      }
}

class CantonAdminTokenAllPermissionsIntegrationTest extends CantonAdminTokenIntegrationTest(true) {

  "A participant configured to use an adminToken" when {
    val securityTag = SecurityTest(property = Authorization, asset = "Ledger API application")
    "a remote console connects with an invalid token" must {
      "reject ledger api calls" taggedAs securityTag.setAttack(
        Attack(
          actor = "Unauthorized ledger API client",
          threat = "An unauthorized application tries to access the ledger API",
          mitigation = "Reject ledger api access of a client with an invalid JWT.",
        )
      ) in { implicit env =>
        import env.*

        val invalid = rp("invalid1")

        // will fail (as party endpoint requires admin privs)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          invalid.ledger_api.parties.list(),
          _.warningMessage should include("The Token's Signature resulted invalid "),
          _.warningMessage should include("The Token's Signature resulted invalid "),
          _.warningMessage should include regex
            """UNAUTHENTICATED\(6,.{8}\): The command is missing a \(valid\) JWT token""".r,
          _.commandFailureMessage should include("UNAUTHENTICATED"),
        )
      }
    }

    "a remote console connects without a token" must {
      "reject ledger api calls" taggedAs securityTag.setAttack(
        Attack(
          actor = "Unauthorized ledger API client",
          threat = "An unauthorized application tries to access the ledger API",
          mitigation = "Reject ledger api access of a client without a JWT.",
        )
      ) in { implicit env =>
        import env.*

        val noToken = rp("noToken1")

        // will fail (as party endpoint requires admin privs)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          noToken.ledger_api.parties.list(),
          _.warningMessage should include regex
            """UNAUTHENTICATED\(6,.{8}\): The command is missing a \(valid\) JWT token""".r,
          _.commandFailureMessage should include("UNAUTHENTICATED"),
        )
      }
    }

    "a remote console connects with a valid token" can {

      "authorize ledger api calls that use an admin token to access public method" taggedAs securityTag
        .setHappyCase(
          "Ledger API client in remote console works with correct admin token"
        ) in { implicit env =>
        import env.*

        val admin = rp("admin1")
        admin.ledger_api.state.end()
      }

      "authorize ledger api calls that use an admin token to access participant party data" taggedAs securityTag
        .setHappyCase(
          "Ledger API client in remote console works with correct admin token"
        ) in { implicit env =>
        import env.*

        val admin = rp("admin1")
        admin.ledger_api.state.acs.of_party(participant1.adminParty)
      }

      "authorize ledger api calls that use an admin token to access an admin method" taggedAs securityTag
        .setHappyCase(
          "Ledger API client in remote console works with correct admin token"
        ) in { implicit env =>
        import env.*

        val admin = rp("admin1")
        admin.ledger_api.parties.list()
      }

      "authorize ledger api calls that use a regular token" taggedAs securityTag.setHappyCase(
        "Ledger API client in remote console works with correct token"
      ) in { implicit env =>
        import env.*

        val valid = rp("valid1")
        valid.ledger_api.parties.list()
      }

      "authorize ledger api calls that use an admin token to access an all-party data" taggedAs securityTag
        .setHappyCase(
          "Ledger API client in remote console works with correct admin token"
        ) in { implicit env =>
        import env.*

        val admin = rp("admin1")
        val valid = rp("valid1")

        val otherParty = valid.ledger_api.parties.allocate("other-party").party
        admin.ledger_api.state.acs.of_party(otherParty)
      }
    }
  }
}

class CantonAdminTokenFewPermissionsIntegrationTest extends CantonAdminTokenIntegrationTest(false) {

  "A participant configured to use an adminToken" when {
    val securityTag = SecurityTest(property = Authorization, asset = "Ledger API application")

    "a remote console connects with a valid token" can {
      "authorize ledger api calls that use an admin token to access public method" taggedAs securityTag
        .setHappyCase(
          "Ledger API client in remote console works with correct admin token"
        ) in { implicit env =>
        import env.*

        val admin = rp("admin1")
        admin.ledger_api.state.end()
      }

      "authorize ledger api calls that use an admin token to access participant party data" taggedAs securityTag
        .setHappyCase(
          "Ledger API client in remote console works with correct admin token"
        ) in { implicit env =>
        import env.*

        val admin = rp("admin1")
        admin.ledger_api.state.acs.of_party(participant1.adminParty)
      }

    }

    "a remote console connects with a valid token" must {
      "reject ledger api calls that use an admin token to access admin method" taggedAs securityTag
        .setAttack(
          Attack(
            actor = "Ledger API client with a token not entitled to perform admin operations",
            threat = "An unauthorized application tries to access the ledger API",
            mitigation = "Reject client attempt to access ledger api admin operation.",
          )
        ) in { implicit env =>
        import env.*

        val admin = rp("admin1")

        // will fail (as party endpoint requires admin privs)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          admin.ledger_api.parties.list(),
          _.warningMessage should include regex
            """PERMISSION_DENIED\(7,.{8}\): Claims do not authorize the use of administrative services""".r,
          _.commandFailureMessage should include("PERMISSION_DENIED"),
        )
      }

      "reject ledger api calls that use an admin token to access all-party data" taggedAs securityTag
        .setAttack(
          Attack(
            actor = "Ledger API client with a token not entitled to perform all-party operations",
            threat = "An unauthorized application tries to access the ledger API",
            mitigation = "Reject client attempt to access ledger api all-party operation.",
          )
        ) in { implicit env =>
        import env.*

        val admin = rp("admin1")
        val valid = rp("valid1")

        val otherParty = valid.ledger_api.parties.allocate("other-party").party

        // will fail (as asc endpoint requires privilege to read as all-parties or as a specific party)
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          admin.ledger_api.state.acs.of_party(otherParty),
          _.warningMessage should include regex
            """PERMISSION_DENIED\(7,.{8}\): Claims do not authorize to read data for party""".r,
          _.commandFailureMessage should include("PERMISSION_DENIED"),
        )
      }
    }
  }
}
