// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import com.daml.integrationtest.CantonFixture
import com.daml.ledger.api.domain
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask
import io.grpc.ManagedChannel
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.OneAnd

final class LedgerClientIT extends AsyncWordSpec with Matchers with Inside with CantonFixture {

  private val LedgerId = domain.LedgerId(config.ledgerIds.head)

  lazy val channel = config.channel(ports.head)

  private val ClientConfiguration = LedgerClientConfiguration(
    applicationId = applicationId.getOrElse(""),
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    token = None,
  )

  "the ledger client" should {
    "retrieve the ledger ID" in {
      for {
        client <- LedgerClient(channel, ClientConfiguration)
      } yield {
        client.ledgerId should be(LedgerId)
      }
    }

    "make some requests" in {
      val partyName = CantonFixture.freshName("Alice")
      for {
        client <- LedgerClient(channel, ClientConfiguration)
        // The request type is irrelevant here; the point is that we can make some.
        allocatedParty <- client.partyManagementClient
          .allocateParty(hint = Some(partyName), displayName = None)
        retrievedParties <- client.partyManagementClient
          .getParties(OneAnd(Ref.Party.assertFromString(allocatedParty.party), Set.empty))
      } yield {
        retrievedParties should be(List(allocatedParty))
      }
    }

    "get api version" in {
      // semantic versioning regex as in: https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
      val semVerRegex =
        """^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"""
      for {
        client <- LedgerClient(channel, ClientConfiguration)
        version <- client.versionClient.getApiVersion()
      } yield {
        version should fullyMatch regex semVerRegex
      }
    }

    "identity provider config" should {
      def freshConfig(): domain.IdentityProviderConfig = domain.IdentityProviderConfig(
        domain.IdentityProviderId.Id(
          Ref.LedgerString.assertFromString(CantonFixture.freshName("abcd"))
        ),
        isDeactivated = false,
        domain.JwksUrl.assertFromString("http://jwks.some.domain:9999/jwks"),
        CantonFixture.freshName("SomeUser"),
        Some(CantonFixture.freshName("SomeAudience")),
      )

      def updateConfig(config: domain.IdentityProviderConfig) = config.copy(
        isDeactivated = true,
        jwksUrl = domain.JwksUrl("http://someotherurl"),
        issuer = CantonFixture.freshName("ANewIssuer"),
        audience = Some(CantonFixture.freshName("ChangedAudience")),
      )

      "create an identity provider" in {
        val config = freshConfig()
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          createdConfig <- client.identityProviderConfigClient.createIdentityProviderConfig(
            config,
            None,
          )
        } yield {
          createdConfig should be(config)
        }
      }
      "get an identity provider" in {
        val config = freshConfig()
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          _ <- client.identityProviderConfigClient.createIdentityProviderConfig(config, None)
          respConfig <- client.identityProviderConfigClient.getIdentityProviderConfig(
            config.identityProviderId,
            None,
          )
        } yield {
          respConfig should be(config)
        }
      }
      "update an identity provider" in {
        val config = freshConfig()
        val updatedConfig = updateConfig(config)
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          _ <- client.identityProviderConfigClient.createIdentityProviderConfig(config, None)
          respConfig <- client.identityProviderConfigClient.updateIdentityProviderConfig(
            updatedConfig,
            FieldMask(Seq("is_deactivated", "jwks_url", "issuer", "audience")),
            None,
          )
          queriedConfig <- client.identityProviderConfigClient.getIdentityProviderConfig(
            config.identityProviderId,
            None,
          )
        } yield {
          respConfig should be(updatedConfig)
          queriedConfig should be(updatedConfig)
        }
      }

      "list identity providers" in {
        val config = freshConfig()
        val updatedConfig = updateConfig(config)
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          before <- client.identityProviderConfigClient.listIdentityProviderConfigs(None)
          config1 <- client.identityProviderConfigClient.createIdentityProviderConfig(config, None)
          config2 <- client.identityProviderConfigClient.createIdentityProviderConfig(
            updatedConfig.copy(identityProviderId =
              domain.IdentityProviderId.Id(
                Ref.LedgerString.assertFromString("AnotherIdentityProvider")
              )
            ),
            None,
          )
          after <- client.identityProviderConfigClient.listIdentityProviderConfigs(None)
        } yield {
          (after.toSet -- before) should contain theSameElementsAs Set(config2, config1)
        }
      }

      "delete identity provider" in {
        val config = freshConfig()
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          before <- client.identityProviderConfigClient.listIdentityProviderConfigs(None)
          config1 <- client.identityProviderConfigClient.createIdentityProviderConfig(config, None)
          _ <- client.identityProviderConfigClient.deleteIdentityProviderConfig(
            config1.identityProviderId,
            None,
          )
          after <- client.identityProviderConfigClient.listIdentityProviderConfigs(None)
        } yield {
          before.toSet should be(after.toSet)
        }
      }

    }

    "shut down the channel when closed" in {
      for {
        client <- LedgerClient(channel, ClientConfiguration)
      } yield {
        inside(channel) { case channel: ManagedChannel =>
          channel.isShutdown should be(false)
          channel.isTerminated should be(false)

          client.close()

          channel.isShutdown should be(true)
          channel.isTerminated should be(true)
        }
      }
    }
  }
}
