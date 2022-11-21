// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId, JwksUrl}
import com.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

import java.sql.SQLException
import java.util.UUID

private[backend] trait StorageBackendTestsIDPConfig
    extends Matchers
    with Inside
    with StorageBackendSpec
    with OptionValues {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (Identity Provider Config)"

  private def tested = backend.identityProviderStorageBackend

  it should "create and load unchanged an identity provider config" in {
    val cfg = config()
    executeSql(tested.createIdentityProviderConfig(cfg))
    executeSql(tested.getIdentityProviderConfig(cfg.identityProviderId)) shouldBe Some(cfg)
  }

  it should "delete an identity provider config" in {
    val cfg = config()
    executeSql(tested.createIdentityProviderConfig(cfg))
    executeSql(tested.getIdentityProviderConfig(cfg.identityProviderId)) shouldBe Some(cfg)
    executeSql(tested.deleteIdentityProviderConfig(cfg.identityProviderId))
    executeSql(tested.getIdentityProviderConfig(cfg.identityProviderId)) shouldBe None
  }

  it should "update existing identity provider config's isDeactivated attribute" in {
    val cfg = config().copy(isDeactivated = false)
    executeSql(tested.createIdentityProviderConfig(cfg))
    // deactivate
    executeSql(tested.updateIsDeactivated(cfg.identityProviderId, true)) shouldBe true
    executeSql(
      tested.getIdentityProviderConfig(cfg.identityProviderId)
    ).value.isDeactivated shouldBe true
    // activate again
    executeSql(tested.updateIsDeactivated(cfg.identityProviderId, false)) shouldBe true
    executeSql(
      tested.getIdentityProviderConfig(cfg.identityProviderId)
    ).value.isDeactivated shouldBe false
  }

  it should "update existing identity provider config's jwksURL attribute" in {
    val cfg = config()
    executeSql(tested.createIdentityProviderConfig(cfg))
    val newJwksUrl = JwksUrl("http://example.com/jwks2.json")
    executeSql(tested.updateJwksUrl(cfg.identityProviderId, newJwksUrl)) shouldBe true
    executeSql(
      tested.getIdentityProviderConfig(cfg.identityProviderId)
    ).value.jwksUrl shouldBe newJwksUrl
  }

  it should "update existing identity provider config's issuer attribute" in {
    val cfg = config()
    executeSql(tested.createIdentityProviderConfig(cfg))
    val newIssuer = UUID.randomUUID().toString
    executeSql(tested.updateIssuer(cfg.identityProviderId, newIssuer)) shouldBe true
    executeSql(
      tested.getIdentityProviderConfig(cfg.identityProviderId)
    ).value.issuer shouldBe newIssuer
  }

  it should "check if identity provider config's issuer exists" in {
    val cfg = config()
    executeSql(tested.identityProviderConfigByIssuerExists(cfg.issuer)) shouldBe false
    executeSql(tested.createIdentityProviderConfig(cfg))
    executeSql(tested.identityProviderConfigByIssuerExists(cfg.issuer)) shouldBe true
  }

  it should "check if identity provider config by id exists" in {
    val cfg = config()
    executeSql(tested.idpConfigByIdExists(cfg.identityProviderId)) shouldBe false
    executeSql(tested.createIdentityProviderConfig(cfg))
    executeSql(tested.idpConfigByIdExists(cfg.identityProviderId)) shouldBe true
  }

  it should "fail to update issuer for non existing identity provider config" in {
    executeSql(tested.updateIssuer(randomId(), "whatever")) shouldBe false
    executeSql(tested.updateIssuer(randomId(), "")) shouldBe false
  }

  it should "fail to update isDeactivated for non existing identity provider config" in {
    executeSql(tested.updateIsDeactivated(randomId(), true)) shouldBe false
    executeSql(tested.updateIsDeactivated(randomId(), false)) shouldBe false
  }

  it should "fail to update jwksURL for non existing identity provider config" in {
    executeSql(
      tested.updateJwksUrl(randomId(), JwksUrl("http://example.com/jwks.json"))
    ) shouldBe false
    executeSql(
      tested.updateJwksUrl(randomId(), JwksUrl("http://example2.com/jwks.json"))
    ) shouldBe false
  }

  it should "fail to update identity provider config issuer attribute to non-unique issuer" in {
    val cfg1 = config()
    val cfg2 = config()
    executeSql(tested.createIdentityProviderConfig(cfg1))
    executeSql(tested.createIdentityProviderConfig(cfg2))
    assertThrows[SQLException] {
      executeSql(tested.updateIssuer(cfg1.identityProviderId, cfg2.issuer))
    }
  }

  it should "fail to create identity provider config with non-unique issuer" in {
    val cfg1 = config()
    val cfg2 = config()
    executeSql(tested.createIdentityProviderConfig(cfg1))
    assertThrows[SQLException] {
      executeSql(tested.createIdentityProviderConfig(cfg2.copy(issuer = cfg1.issuer)))
    }
  }

  it should "fail to create identity provider config with non-unique id" in {
    val cfg1 = config()
    val cfg2 = config()
    executeSql(tested.createIdentityProviderConfig(cfg1))
    assertThrows[SQLException] {
      executeSql(
        tested.createIdentityProviderConfig(cfg2.copy(identityProviderId = cfg1.identityProviderId))
      )
    }
  }

  it should "get all identity provider configs ordered by id" in {
    val cfg1 = config()
    val cfg2 = config()
    val cfg3 = config()
    executeSql(tested.createIdentityProviderConfig(cfg1))
    executeSql(tested.createIdentityProviderConfig(cfg2))
    executeSql(tested.createIdentityProviderConfig(cfg3))

    executeSql(
      tested.listIdentityProviderConfigs()
    ) should contain theSameElementsAs Vector(cfg1, cfg2, cfg3)
  }

  private def config() = {
    IdentityProviderConfig(
      identityProviderId = randomId(),
      isDeactivated = false,
      jwksUrl = JwksUrl.assertFromString("http://example.com/jwks.json"),
      issuer = UUID.randomUUID().toString,
    )
  }

  private def randomId() = {
    val id = UUID.randomUUID().toString
    IdentityProviderId.Id(Ref.LedgerString.assertFromString(id))
  }

}
