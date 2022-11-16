// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.lf.data.Ref

import java.net.URL
import java.sql.Connection

trait IdentityProviderStorageBackend extends IdentityProviderCheckStorageBackend {
  def createIdentityProviderConfig(
      identityProviderConfig: IdentityProviderConfig
  )(connection: Connection): Unit

  def deleteIdentityProviderConfig(id: Ref.IdentityProviderId.Id)(connection: Connection): Boolean

  def getIdentityProviderConfig(id: Ref.IdentityProviderId.Id)(
      connection: Connection
  ): Option[IdentityProviderConfig]

  def listIdentityProviderConfigs()(
      connection: Connection
  ): Vector[IdentityProviderConfig]

  def updateIssuer(id: Ref.IdentityProviderId.Id, newIssuer: String)(
      connection: Connection
  ): Boolean

  def updateJwksURL(id: Ref.IdentityProviderId.Id, jwksURL: URL)(
      connection: Connection
  ): Boolean

  def updateIsDeactivated(id: Ref.IdentityProviderId.Id, isDeactivated: Boolean)(connection: Connection): Boolean

  def idpConfigByIssuerExists(issuer: String)(connection: Connection): Boolean

}

object IdentityProviderStorageBackend {}
