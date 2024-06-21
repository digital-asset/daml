// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.localstore

import anorm.SqlParser.{bool, int, str}
import anorm.{RowParser, SqlParser, ~}
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.ledger.api.domain.{
  IdentityProviderConfig,
  IdentityProviderId,
  JwksUrl,
}
import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*

import java.sql.Connection

object IdentityProviderStorageBackendImpl extends IdentityProviderStorageBackend {

  private val IntParser: RowParser[Int] =
    int("dummy") map { i => i }

  private val IdpConfigRecordParser: RowParser[IdentityProviderConfig] = {
    import com.digitalasset.canton.platform.store.backend.Conversions.bigDecimalColumnToBoolean
    str("identity_provider_id") ~
      bool("is_deactivated") ~
      str("jwks_url") ~
      str("issuer") ~
      str("audience").? map {
        case identityProviderId ~ isDeactivated ~ jwksUrl ~ issuer ~ audience =>
          IdentityProviderConfig(
            identityProviderId = IdentityProviderId.Id.assertFromString(identityProviderId),
            isDeactivated = isDeactivated,
            jwksUrl = JwksUrl.assertFromString(jwksUrl),
            issuer = issuer,
            audience = audience,
          )
      }
  }

  override def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig)(
      connection: Connection
  ): Unit = {
    val identityProviderId = identityProviderConfig.identityProviderId.value: String
    val isDeactivated = identityProviderConfig.isDeactivated
    val jwksUrl = identityProviderConfig.jwksUrl.value
    val issuer = identityProviderConfig.issuer
    val audience = identityProviderConfig.audience
    discard(SQL"""
       INSERT INTO lapi_identity_provider_config (identity_provider_id, is_deactivated, jwks_url, issuer, audience)
       VALUES ($identityProviderId, $isDeactivated, $jwksUrl, $issuer, $audience)
     """.execute()(connection))
  }

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(
      connection: Connection
  ): Boolean = {
    val updatedRowsCount =
      SQL"""
         DELETE FROM lapi_identity_provider_config WHERE identity_provider_id = ${id.value: String}
         """.executeUpdate()(connection)
    updatedRowsCount == 1
  }

  override def getIdentityProviderConfig(id: IdentityProviderId.Id)(
      connection: Connection
  ): Option[IdentityProviderConfig] = {
    SQL"""
       SELECT identity_provider_id, is_deactivated, jwks_url, issuer, audience
       FROM lapi_identity_provider_config
       WHERE identity_provider_id = ${id.value: String}
       """
      .as(IdpConfigRecordParser.singleOpt)(connection)
  }

  override def listIdentityProviderConfigs()(
      connection: Connection
  ): Vector[IdentityProviderConfig] = {
    SQL"""
       SELECT identity_provider_id, is_deactivated, jwks_url, issuer, audience
       FROM lapi_identity_provider_config
       ORDER BY identity_provider_id
       """
      .asVectorOf(IdpConfigRecordParser)(connection)
  }

  override def identityProviderConfigByIssuerExists(
      ignoreId: IdentityProviderId.Id,
      issuer: String,
  )(connection: Connection): Boolean = {
    import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val res: Seq[_] =
      SQL"""
         SELECT 1 AS dummy
         FROM lapi_identity_provider_config t
         WHERE
            t.issuer = $issuer AND
            identity_provider_id != ${ignoreId.value: String}
         """.asVectorOf(IntParser)(connection)
    assert(res.length <= 1)
    res.length == 1
  }

  override def idpConfigByIdExists(id: IdentityProviderId.Id)(connection: Connection): Boolean = {
    import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val res: Seq[_] =
      SQL"""
           SELECT 1 AS dummy
           FROM lapi_identity_provider_config t
           WHERE t.identity_provider_id = ${id.value: String}
           """.asVectorOf(IntParser)(connection)
    assert(res.length <= 1)
    res.length == 1
  }

  override def updateIssuer(id: IdentityProviderId.Id, newIssuer: String)(
      connection: Connection
  ): Boolean = {
    val rowsUpdated =
      SQL"""
         UPDATE lapi_identity_provider_config
         SET issuer  = $newIssuer
         WHERE identity_provider_id = ${id.value: String}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateJwksUrl(id: IdentityProviderId.Id, jwksUrl: JwksUrl)(
      connection: Connection
  ): Boolean = {
    val rowsUpdated =
      SQL"""
         UPDATE lapi_identity_provider_config
         SET jwks_url = ${jwksUrl.value}
         WHERE identity_provider_id = ${id.value: String}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateAudience(id: IdentityProviderId.Id, audience: Option[String])(
      connection: Connection
  ): Boolean = {
    val audienceSql = audience match {
      case Some(aud) =>
        cSQL"""SET audience = $aud"""
      case None =>
        cSQL"""SET audience = NULL"""
    }
    val rowsUpdated =
      SQL"""
         UPDATE lapi_identity_provider_config
         $audienceSql
         WHERE identity_provider_id = ${id.value: String}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def updateIsDeactivated(id: IdentityProviderId.Id, isDeactivated: Boolean)(
      connection: Connection
  ): Boolean = {
    val rowsUpdated =
      SQL"""
         UPDATE lapi_identity_provider_config
         SET is_deactivated  = $isDeactivated
         WHERE identity_provider_id = ${id.value: String}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

  override def countIdentityProviderConfigs()(connection: Connection): Int = {
    SQL"SELECT count(*) AS identity_provider_configs_count from lapi_identity_provider_config"
      .as(SqlParser.int("identity_provider_configs_count").single)(connection)
  }

  override def getIdentityProviderConfigByIssuer(
      issuer: String
  )(connection: Connection): Option[IdentityProviderConfig] = {
    SQL"""
       SELECT identity_provider_id, is_deactivated, jwks_url, issuer, audience
       FROM lapi_identity_provider_config
       WHERE issuer = ${issuer}
       """
      .as(IdpConfigRecordParser.singleOpt)(connection)
  }
}
