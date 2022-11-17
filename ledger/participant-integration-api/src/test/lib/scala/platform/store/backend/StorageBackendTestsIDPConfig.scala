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

  it should "TODO" in {

    val id = UUID.randomUUID().toString
    executeSql(
      tested.createIdentityProviderConfig(
        IdentityProviderConfig(
          identityProviderId = Ref.IdentityProviderId.Id(Ref.LedgerString.assertFromString(id)),
          isDeactivated = false,
          jwksURL = JwksUrl("http://example.com/jwks.json"),
          issuer = UUID.randomUUID().toString,
        )
      )
    )
  }

}
