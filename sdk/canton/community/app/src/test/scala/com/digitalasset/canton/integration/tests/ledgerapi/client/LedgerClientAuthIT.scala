// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.client

import com.daml.grpc.GrpcException
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.SuppressionRules.AuthInterceptorSuppressionRule
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.{
  CantonFixture,
  CreatesParties,
  CreatesUsers,
}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
}

import java.util.UUID

final class LedgerClientAuthIT extends CantonFixture with CreatesParties with CreatesUsers {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val clientConfigurationWithoutToken = LedgerClientConfiguration(
    userId = classOf[LedgerClientAuthIT].getSimpleName,
    commandClient = CommandClientConfiguration.default,
    token = () => None,
  )

  private val clientConfiguration = clientConfigurationWithoutToken.copy(
    token = () => Some(toHeader(adminToken))
  )

  private val someParty = UUID.randomUUID.toString
  private val someUser = UUID.randomUUID.toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    val token = Some(toHeader(adminToken))
    createPrerequisiteParties(token, List(someParty))(
      directExecutionContext
    )
    createPrerequisiteUsers(
      token,
      List(PrerequisiteUser(someUser, actAsParties = List(someParty))),
    )(
      directExecutionContext
    )
  }

  "the ledger client" when {
    "it has a read-only token" should {
      "fail to conduct an admin operation with plain user token" in {
        (env: TestConsoleEnvironment) =>
          loggerFactory.suppress(AuthInterceptorSuppressionRule) {
            import env.*
            (for {
              client <- LedgerClient(channel, clientConfiguration, loggerFactory)
              exception <- client.partyManagementClient
                .allocateParty(
                  hint = Some("Bob"),
                  token = Some(toHeader(standardToken(someParty))),
                )
                .failed
            } yield {
              inside(exception) { case GrpcException.PERMISSION_DENIED() =>
                succeed
              }
            }).futureValue
          }
      }

      "succeed in conducting an admin operation with an admin token" in { env =>
        import env.*
        val partyName = "Carol"
        (for {
          client <- LedgerClient(channel, clientConfiguration, loggerFactory)
          allocatedParty <- client.partyManagementClient
            .allocateParty(
              hint = Some(partyName),
              token = Some(toHeader(adminToken)),
            )
        } yield {
          allocatedParty.isLocal should be(true)
        }).futureValue
      }
    }

    "it does not have a token" should {
      "fail to construct" in { env =>
        import env.*
        (for {
          exception <- LedgerClient(channel, clientConfigurationWithoutToken, loggerFactory).failed
        } yield {
          inside(exception) { case GrpcException.UNAUTHENTICATED() =>
            succeed
          }
        }).futureValue
      }
    }
  }

}
