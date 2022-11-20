// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId, JwksUrl}

import java.sql.Connection

trait IdentityProviderStorageBackend extends IdentityProviderCheckStorageBackend {
  def createIdentityProviderConfig(
      identityProviderConfig: IdentityProviderConfig
  )(connection: Connection): Unit

  def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(connection: Connection): Boolean

  def getIdentityProviderConfig(id: IdentityProviderId.Id)(
      connection: Connection
  ): Option[IdentityProviderConfig]

  def listIdentityProviderConfigs()(
      connection: Connection
  ): Vector[IdentityProviderConfig]

  def updateIssuer(id: IdentityProviderId.Id, newIssuer: String)(
      connection: Connection
  ): Boolean

  def updateJwksURL(id: IdentityProviderId.Id, jwksURL: JwksUrl)(
      connection: Connection
  ): Boolean

  def updateIsDeactivated(id: IdentityProviderId.Id, isDeactivated: Boolean)(
      connection: Connection
  ): Boolean

  def idpConfigByIssuerExists(issuer: String)(connection: Connection): Boolean

}

object IdentityProviderStorageBackend {}
