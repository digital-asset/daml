// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.topology

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.api.{IdentityProviderConfig, IdentityProviderId, JwksUrl}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.field_mask.FieldMask

import java.util.UUID

trait IdentityProviderConfigIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  var alice: PartyId = _
  var bob: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, daName)

        alice = participant1.parties.enable("alice")
        bob = participant1.parties.enable("bob")
      }

  def randomId(): IdentityProviderId.Id = {
    val id = UUID.randomUUID().toString
    IdentityProviderId.Id(Ref.LedgerString.assertFromString(id))
  }

  "managing identity provider configs" should {

    val jwksUrlString = "https://jwks:900"
    val someJwksUrl = JwksUrl.assertFromString(jwksUrlString)
    val someAudience = Option("someAudience")

    "create new identity provider config" in { implicit env =>
      import env.*
      val identityProviderId = randomId()
      val someIssuer = UUID.randomUUID().toString

      val config = participant1.ledger_api.identity_provider_config.create(
        identityProviderId = identityProviderId.toRequestString,
        jwksUrl = jwksUrlString,
        issuer = someIssuer,
        audience = someAudience,
      )

      config shouldBe IdentityProviderConfig(
        identityProviderId,
        isDeactivated = false,
        someJwksUrl,
        someIssuer,
        someAudience,
      )
    }

    "update identity provider config" in { implicit env =>
      import env.*

      val identityProviderId = randomId()
      val someIssuer = UUID.randomUUID().toString

      val updatedDeactivation = true
      val updatedJwksUrl = "http://newjkwks:9000"
      val updatedAudience = "updatedAudience"
      val updatedIssuer = "updatedIssuer"

      val originalConfig = participant1.ledger_api.identity_provider_config.create(
        identityProviderId = identityProviderId.toRequestString,
        jwksUrl = jwksUrlString,
        issuer = someIssuer,
        audience = someAudience,
      )

      originalConfig shouldBe IdentityProviderConfig(
        identityProviderId,
        isDeactivated = false,
        someJwksUrl,
        someIssuer,
        someAudience,
      )

      val updatedConfig = participant1.ledger_api.identity_provider_config.update(
        identityProviderId.toRequestString,
        isDeactivated = updatedDeactivation,
        jwksUrl = updatedJwksUrl,
        audience = Some(updatedAudience),
        issuer = updatedIssuer,
        updateMask = FieldMask(Seq("is_deactivated", "jwks_url", "audience", "issuer")),
      )

      updatedConfig shouldBe IdentityProviderConfig(
        identityProviderId,
        updatedDeactivation,
        JwksUrl(updatedJwksUrl),
        updatedIssuer,
        Some(updatedAudience),
      )
    }

    "delete identity provider config (get and list)" in { implicit env =>
      import env.*

      val identityProviderId = randomId()
      val someIssuer = UUID.randomUUID().toString

      /* create a new provider */
      participant1.ledger_api.identity_provider_config.create(
        identityProviderId = identityProviderId.toRequestString,
        jwksUrl = jwksUrlString,
        issuer = someIssuer,
        audience = someAudience,
      )

      /* list provider */
      participant1.ledger_api.identity_provider_config
        .list()
        .filter(_.identityProviderId == identityProviderId) should have length 1

      /* get by id */
      participant1.ledger_api.identity_provider_config.get(
        identityProviderId.toRequestString
      ) shouldBe IdentityProviderConfig(
        identityProviderId,
        isDeactivated = false,
        someJwksUrl,
        someIssuer,
        someAudience,
      )

      /* delete provider */
      participant1.ledger_api.identity_provider_config.delete(
        identityProviderId.toRequestString
      )

      /* list provider */
      participant1.ledger_api.identity_provider_config
        .list()
        .filter(_.identityProviderId == identityProviderId) should have length 0
    }

  }
}

class IdentityProviderConfigReferenceIntegrationTestPostgres
    extends IdentityProviderConfigIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
