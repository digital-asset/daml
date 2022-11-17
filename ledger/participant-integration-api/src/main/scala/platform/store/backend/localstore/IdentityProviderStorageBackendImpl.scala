// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import anorm.SqlParser.{bool, int, str}
import anorm.{RowParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain.{IdentityProviderConfig, JwksUrl}
import com.daml.lf.data.Ref.IdentityProviderId
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.scalautil.Statement.discard

import java.sql.Connection

object IdentityProviderStorageBackendImpl extends IdentityProviderStorageBackend {

  private val IntParser0: RowParser[Int] =
    int("dummy") map { i => i }

  private val IDPConfigRecordParser: RowParser[(String, Boolean, String, String)] = {
    import com.daml.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    str("identity_provider_id") ~
      bool("is_deactivated") ~
      str("jwks_url") ~
      str("issuer") map { case identityProviderId ~ isDeactivated ~ jwksURL ~ issuer =>
        (identityProviderId, isDeactivated, jwksURL, issuer)
      }
  }

  override def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig)(
      connection: Connection
  ): Unit = {
    val identityProviderId = identityProviderConfig.identityProviderId.value: String
    val isDeactivated = identityProviderConfig.isDeactivated
    val jwksUrl = identityProviderConfig.jwksURL.toString
    val issuer = identityProviderConfig.issuer
    discard(SQL"""
       INSERT INTO participant_identity_provider_config (identity_provider_id, is_deactivated, jwks_url, issuer)
       VALUES ($identityProviderId, $isDeactivated, $jwksUrl, $issuer)
     """.execute()(connection))
  }

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(
      connection: Connection
  ): Boolean = {
    val updatedRowsCount =
      SQL"""
         DELETE FROM participant_identity_provider_config WHERE identity_provider_id = ${id.value: String}
         """.executeUpdate()(connection)
    updatedRowsCount == 1
  }

  override def getIdentityProviderConfig(id: IdentityProviderId.Id)(
      connection: Connection
  ): Option[IdentityProviderConfig] = {
    SQL"""
       SELECT identity_provider_id, is_deactivated, jwks_url, issuer
       FROM participant_identity_provider_config
       WHERE identity_provider_id = ${id.value: String}
       """
      .as(IDPConfigRecordParser.singleOpt)(connection)
      .map { case (identityProviderId, isDeactivated, jwksURL, issuer) =>
        IdentityProviderConfig(
          identityProviderId = IdentityProviderId.Id.assertFromString(identityProviderId),
          isDeactivated = isDeactivated,
          jwksURL = JwksUrl.assertFromString(jwksURL),
          issuer = issuer,
        )
      }
  }

  override def listIdentityProviderConfigs()(
      connection: Connection
  ): Vector[IdentityProviderConfig] = {
    SQL"""
       SELECT identity_provider_id, is_deactivated, jwks_url, issuer
       FROM participant_identity_provider_config
       """
      .asVectorOf(IDPConfigRecordParser)(connection)
      .map { case (identityProviderId, isDeactivated, jwksURL, issuer) =>
        IdentityProviderConfig(
          identityProviderId = IdentityProviderId.Id.assertFromString(identityProviderId),
          isDeactivated = isDeactivated,
          jwksURL = JwksUrl.assertFromString(jwksURL),
          issuer = issuer,
        )
      }
  }

  override def idpConfigByIssuerExists(issuer: String)(connection: Connection): Boolean = {
    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val res: Seq[_] =
      SQL"""
         SELECT 1 AS dummy
         FROM participant_identity_provider_config idpcfg
         WHERE idpcfg.issuer = $issuer
         """.asVectorOf(IntParser0)(connection)
    assert(res.length <= 1)
    res.length == 1
  }

  override def idpConfigByIdExists(id: IdentityProviderId.Id)(connection: Connection): Boolean =
    IdentityProviderCheckStorageBackendImpl.idpConfigByIdExists(id)(connection)

  override def updateIssuer(id: IdentityProviderId.Id, newIssuer: String)(
      connection: Connection
  ): Boolean = {
    val rowsUpdated =
      SQL"""
         UPDATE participant_identity_provider_config
         SET issuer  = $newIssuer
         WHERE
             WHERE identity_provider_id = ${id.value: String}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateJwksURL(id: IdentityProviderId.Id, jwksURL: JwksUrl)(
      connection: Connection
  ): Boolean = {
    val rowsUpdated =
      SQL"""
         UPDATE participant_identity_provider_config
         SET jwks_url  = ${jwksURL.value}
         WHERE
             WHERE identity_provider_id = ${id.value: String}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateIsDeactivated(id: IdentityProviderId.Id, isDeactivated: Boolean)(
      connection: Connection
  ): Boolean = {
    val rowsUpdated =
      SQL"""
         UPDATE participant_identity_provider_config
         SET is_deactivated  = $isDeactivated
         WHERE identity_provider_id = ${id.value: String}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }
}
