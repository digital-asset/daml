// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.api.domain.{IdentityProviderConfig, JwksUrl}
import com.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

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
    executeSql(tested.updateIsDeactivated(cfg.identityProviderId, true))
    executeSql(
      tested.getIdentityProviderConfig(cfg.identityProviderId)
    ).value.isDeactivated shouldBe true
  }

  it should "update existing identity provider config's jwksURL attribute" in {
    val cfg = config()
    executeSql(tested.createIdentityProviderConfig(cfg))
    val newJwksUrl = JwksUrl("http://example.com/jwks2.json")
    executeSql(tested.updateJwksURL(cfg.identityProviderId, newJwksUrl))
    executeSql(
      tested.getIdentityProviderConfig(cfg.identityProviderId)
    ).value.jwksURL shouldBe newJwksUrl
  }

  it should "update existing identity provider config's issuer attribute" in {
    val cfg = config()
    executeSql(tested.createIdentityProviderConfig(cfg))
    val newIssuer = UUID.randomUUID().toString
    executeSql(tested.updateIssuer(cfg.identityProviderId, newIssuer))
    executeSql(
      tested.getIdentityProviderConfig(cfg.identityProviderId)
    ).value.issuer shouldBe newIssuer
  }

  it should "fail to update non existing identity provider config" in {}
  it should "fail to update identity provider config issuer attribute to non-unique issuer" in {}
  it should "fail to create identity provider config with non-unique issuer" in {}
  it should "get all identity provider configs ordered by id" in {}

  def config() = {
    val id = UUID.randomUUID().toString
    IdentityProviderConfig(
      identityProviderId = Ref.IdentityProviderId.Id(Ref.LedgerString.assertFromString(id)),
      isDeactivated = false,
      jwksURL = JwksUrl.assertFromString("http://example.com/jwks.json"),
      issuer = UUID.randomUUID().toString,
    )
  }

}
