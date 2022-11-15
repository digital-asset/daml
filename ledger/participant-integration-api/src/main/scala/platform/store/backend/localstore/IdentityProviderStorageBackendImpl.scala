package com.daml.platform.store.backend.localstore

import anorm.SqlParser.{bool, str}
import anorm.{RowParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.lf.data.Ref.IdentityProviderId
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
import com.daml.scalautil.Statement.discard

import java.net.URL
import java.sql.Connection

object IdentityProviderStorageBackendImpl extends IdentityProviderStorageBackend {

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
          jwksURL = new URL(jwksURL),
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
          jwksURL = new URL(jwksURL),
          issuer = issuer,
        )
      }
  }

}
