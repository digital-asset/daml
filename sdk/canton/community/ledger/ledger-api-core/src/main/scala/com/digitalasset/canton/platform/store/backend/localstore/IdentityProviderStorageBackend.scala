// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.localstore

import com.digitalasset.canton.ledger.api.{IdentityProviderConfig, IdentityProviderId, JwksUrl}

import java.sql.Connection

trait IdentityProviderStorageBackend {
  def createIdentityProviderConfig(
      identityProviderConfig: IdentityProviderConfig
  )(connection: Connection): Unit

  def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(connection: Connection): Boolean

  def getIdentityProviderConfig(id: IdentityProviderId.Id)(
      connection: Connection
  ): Option[IdentityProviderConfig]

  def getIdentityProviderConfigByIssuer(issuer: String)(
      connection: Connection
  ): Option[IdentityProviderConfig]

  def listIdentityProviderConfigs()(
      connection: Connection
  ): Vector[IdentityProviderConfig]

  def updateIssuer(id: IdentityProviderId.Id, newIssuer: String)(
      connection: Connection
  ): Boolean

  def updateJwksUrl(id: IdentityProviderId.Id, jwksUrl: JwksUrl)(
      connection: Connection
  ): Boolean

  def updateAudience(id: IdentityProviderId.Id, audience: Option[String])(
      connection: Connection
  ): Boolean

  def updateIsDeactivated(id: IdentityProviderId.Id, isDeactivated: Boolean)(
      connection: Connection
  ): Boolean

  def identityProviderConfigByIssuerExists(ignoreId: IdentityProviderId.Id, issuer: String)(
      connection: Connection
  ): Boolean

  def countIdentityProviderConfigs()(connection: Connection): Int

  def idpConfigByIdExists(id: IdentityProviderId.Id)(connection: Connection): Boolean

}
