// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.client

import com.daml.jwt.JwksUrl
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ledgerapi.NoAuthPlugin
import com.digitalasset.canton.integration.tests.ledgerapi.fixture.CantonFixture
import com.digitalasset.canton.ledger.api.{IdentityProviderConfig, IdentityProviderId}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import io.grpc.ManagedChannel
import scalaz.OneAnd

final class LedgerClientIT extends CantonFixture {
  registerPlugin(NoAuthPlugin(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val ClientConfiguration = LedgerClientConfiguration(
    userId = classOf[LedgerClientIT].getSimpleName,
    commandClient = CommandClientConfiguration.default,
    token = () => None,
  )

  "the ledger client" should {
    "make some requests" in { env =>
      import env.*
      val partyName = "Alice"
      (for {
        client <- LedgerClient(channel, ClientConfiguration, loggerFactory)
        // The request type is irrelevant here; the point is that we can make some.
        allocatedParty <- client.partyManagementClient.allocateParty(hint = Some(partyName))
        retrievedParties <- client.partyManagementClient
          .getParties(OneAnd(Ref.Party.assertFromString(allocatedParty.party), Set.empty))
      } yield {
        retrievedParties should be(List(allocatedParty))
      }).futureValue
    }

    "get api version" in { env =>
      import env.*
      // semantic versioning regex as in: https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
      val semVerRegex =
        """^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"""
      (for {
        client <- LedgerClient(channel, ClientConfiguration, loggerFactory)
        version <- client.versionClient.getApiVersion()
      } yield {
        version should fullyMatch regex semVerRegex
      }).futureValue
    }

    "shut down the channel when closed" in { env =>
      import env.*
      (for {
        client <- LedgerClient(channel, ClientConfiguration, loggerFactory)
      } yield {
        inside(channel) { case channel: ManagedChannel =>
          channel.isShutdown should be(false)
          channel.isTerminated should be(false)

          client.close()

          channel.isShutdown should be(true)
          channel.isTerminated should be(true)
        }
      }).futureValue
    }

    "identity provider config" should {
      val config = IdentityProviderConfig(
        IdentityProviderId.Id(Ref.LedgerString.assertFromString("abcd")),
        isDeactivated = false,
        JwksUrl.assertFromString("http://jwks.some.domain:9999/jwks"),
        "SomeUser",
        Some("SomeAudience"),
      )

      val updatedConfig = config.copy(
        isDeactivated = true,
        jwksUrl = JwksUrl("http://someotherurl"),
        issuer = "ANewIssuer",
        audience = Some("UpdatedAudience"),
      ) // updating audience value does not appear to work

      "create an identity provider" in { implicit env =>
        import env.*
        // the channel is destroyed in the test above so a new one must be created
        createChannel(participant1)
        (for {
          client <- LedgerClient(channel, ClientConfiguration, loggerFactory)
          createdConfig <- client.identityProviderConfigClient.createIdentityProviderConfig(
            config,
            None,
          )
        } yield {
          createdConfig should be(config)
        }).futureValue
      }
      "delete identity provider" in { env =>
        import env.*
        (for {
          client <- LedgerClient(channel, ClientConfiguration, loggerFactory)
          _ <- client.identityProviderConfigClient.deleteIdentityProviderConfig(
            config.identityProviderId,
            None,
          )
          respConfig <- client.identityProviderConfigClient.listIdentityProviderConfigs(None)
        } yield {
          respConfig.toSet should be(Set.empty)
        }).futureValue
      }
      "get an identity provider" in { env =>
        import env.*
        (for {
          client <- LedgerClient(channel, ClientConfiguration, loggerFactory)
          _ <- client.identityProviderConfigClient.createIdentityProviderConfig(config, None)
          respConfig <- client.identityProviderConfigClient.getIdentityProviderConfig(
            config.identityProviderId,
            None,
          )
        } yield {
          respConfig should be(config)
        }).futureValue
      }
      "update an identity provider" in { env =>
        import env.*
        (for {
          client <- LedgerClient(channel, ClientConfiguration, loggerFactory)
          respConfig <- client.identityProviderConfigClient.updateIdentityProviderConfig(
            updatedConfig,
            FieldMask(Seq("is_deactivated", "jwks_url", "issuer", "audience")),
            None,
          )
          _ <- client.identityProviderConfigClient.deleteIdentityProviderConfig(
            respConfig.identityProviderId,
            None,
          )
        } yield {
          respConfig should be(updatedConfig)
        }).futureValue
      }

      "list identity providers" in { env =>
        import env.*
        (for {
          client <- LedgerClient(channel, ClientConfiguration, loggerFactory)
          config1 <- client.identityProviderConfigClient.createIdentityProviderConfig(config, None)
          config2 <- client.identityProviderConfigClient.createIdentityProviderConfig(
            updatedConfig.copy(identityProviderId =
              IdentityProviderId.Id(Ref.LedgerString.assertFromString("AnotherIdentityProvider"))
            ),
            None,
          )
          respConfig <- client.identityProviderConfigClient.listIdentityProviderConfigs(None)
        } yield {
          respConfig.toSet should contain theSameElementsAs Set(config1, config2)
        }).futureValue
      }

    }
  }

}
