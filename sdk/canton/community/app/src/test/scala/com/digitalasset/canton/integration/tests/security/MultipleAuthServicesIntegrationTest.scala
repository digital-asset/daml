// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Authorization
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest, SecurityTestSuite}
import com.digitalasset.canton.config.AuthServiceConfig
import com.digitalasset.canton.config.CantonRequireTypes.NonEmptyString
import com.digitalasset.canton.console.{
  CommandFailure,
  ConsoleEnvironment,
  ExternalLedgerApiClient,
  LocalParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthServiceJWTSuppressionRule
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.JwtTokenUtilities
import com.digitalasset.canton.topology.PartyId
import monocle.macros.syntax.lens.*

trait MultipleAuthServicesIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with SecurityTestSuite {

  private val mySecret1 = NonEmptyString.tryCreate("pyjama")
  private val mySecret2 = NonEmptyString.tryCreate("underpants")

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") {
        _.focus(_.ledgerApi.authServices)
          .replace(
            Seq(
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = mySecret1,
                  targetAudience = None,
                  targetScope = None,
                ),
              AuthServiceConfig
                .UnsafeJwtHmac256(
                  secret = mySecret2,
                  targetAudience = None,
                  targetScope = None,
                ),
            )
          )
          .focus(_.ledgerApi.adminTokenConfig.adminClaim)
          .replace(true)
      })
      .withSetup { env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
      }

  private def setupPartyAndClient(
      participant: LocalParticipantReference,
      name: String,
      secret: String,
  )(implicit env: ConsoleEnvironment): (ExternalLedgerApiClient, PartyId) = {

    val owner = participant.parties.enable(name)
    val user = participant.ledger_api.users.create(name + "User", Set(owner)).id

    (
      ExternalLedgerApiClient.forReference(
        participant,
        JwtTokenUtilities.buildUnsafeToken(
          secret,
          userId = Some(user),
        ),
      ),
      owner,
    )

  }

  "A participant supporting multiple UnsafeJwtHmac256 authorizations" when {
    val securityTest = SecurityTest(property = Authorization, asset = "Ledger API application")

    "the token is correct" can {

      "use the ledger api through the console" taggedAs securityTest.setHappyCase(
        "Ledger API console client works using the admin token"
      ) in { implicit env =>
        import env.*

        participant1.ledger_api.state.acs.of_all().discard

      }

      "use the ledger api using the first token" taggedAs securityTest.setHappyCase(
        "Ledger API access works with a correct UnsafeJwtHmac256 token of the first auth service"
      ) in { implicit env =>
        import env.*

        val (client, owner) = setupPartyAndClient(participant1, "DonaldT", mySecret1.unwrap)
        client.ledger_api.state.acs.of_party(owner).discard

      }

      "use the ledger api using the second token" taggedAs
        securityTest.setHappyCase(
          "Ledger API access works with a correct UnsafeJwtHmac256 token of the second auth service"
        ) in { implicit env =>
          import env.*

          val (client, owner) = setupPartyAndClient(participant1, "MikeP", mySecret2.unwrap)
          loggerFactory.suppress(AuthServiceJWTSuppressionRule) {
            client.ledger_api.state.acs.of_party(owner).discard
          }

        }

    }

    "token is not valid" can {

      "not access the Ledger Api" taggedAs securityTest.setAttack(
        Attack(
          actor = "Ledger API client with invalid JWT token",
          threat = "An unauthorized application tries to access the ledger API",
          mitigation =
            """Specify authorization in JWT and reject client requests on the server if the specified
                            authorization in the token does not match one of the required ones""",
        )
      ) in { implicit env =>
        import env.*

        val (client, owner) = setupPartyAndClient(participant1, "HarryT", "no-bueno")

        clue("Accessing without valid token") {

          loggerFactory.assertThrowsAndLogs[CommandFailure](
            client.ledger_api.state.acs.of_party(owner),
            _.warningMessage should include("The Token's Signature resulted invalid"),
            _.warningMessage should include("The Token's Signature resulted invalid"),
            _.warningMessage should include regex
              """UNAUTHENTICATED\(6,.{8}\): The command is missing a \(valid\) JWT token""".r,
            _.commandFailureMessage should include("UNAUTHENTICATED"),
          )
        }

      }
    }

  }

}

class MultipleAuthServicesReferenceIntegrationTestPostgres
    extends MultipleAuthServicesIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
