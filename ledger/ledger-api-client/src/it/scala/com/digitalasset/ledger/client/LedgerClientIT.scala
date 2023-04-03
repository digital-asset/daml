// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId, JwksUrl}
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundEach}
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.ledger.runner.common.Config
import com.daml.lf.data.Ref
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.google.protobuf.field_mask.FieldMask
import io.grpc.ManagedChannel
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.OneAnd
import scalaz.syntax.tag._

final class LedgerClientIT
    extends AsyncWordSpec
    with Matchers
    with Inside
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundEach
    with SandboxFixture {

  private val LedgerId =
    domain.LedgerId(s"${classOf[LedgerClientIT].getSimpleName.toLowerCase}-ledger-id")

  private val ClientConfiguration = LedgerClientConfiguration(
    applicationId = classOf[LedgerClientIT].getSimpleName,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    token = None,
  )

  override protected def config: Config = super.config.copy(ledgerId = LedgerId.unwrap)

  "the ledger client" should {
    "retrieve the ledger ID" in {
      for {
        client <- LedgerClient(channel, ClientConfiguration)
      } yield {
        client.ledgerId should be(LedgerId)
      }
    }

    "make some requests" in {
      val partyName = "Alice"
      for {
        client <- LedgerClient(channel, ClientConfiguration)
        // The request type is irrelevant here; the point is that we can make some.
        allocatedParty <- client.partyManagementClient
          .allocateParty(hint = Some(partyName), displayName = None)
        retrievedParties <- client.partyManagementClient
          .getParties(OneAnd(Ref.Party.assertFromString(partyName), Set.empty))
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
        audience = Some("ChangedAudience"),
      )

      "create an identity provider" in {
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
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          config1 <- client.identityProviderConfigClient.createIdentityProviderConfig(config, None)
          config2 <- client.identityProviderConfigClient.createIdentityProviderConfig(
            updatedConfig.copy(identityProviderId =
              IdentityProviderId.Id(Ref.LedgerString.assertFromString("AnotherIdentityProvider"))
            ),
            None,
          )
          respConfig <- client.identityProviderConfigClient.listIdentityProviderConfigs(None)
        } yield {
          respConfig.toSet should contain theSameElementsAs (Set(config2, config1))
        }
      }

      "delete identity provider" in {
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          config1 <- client.identityProviderConfigClient.createIdentityProviderConfig(config, None)
          _ <- client.identityProviderConfigClient.deleteIdentityProviderConfig(
            config1.identityProviderId,
            None,
          )
          respConfig <- client.identityProviderConfigClient.listIdentityProviderConfigs(None)
        } yield {
          respConfig.toSet should be(Set.empty)
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
