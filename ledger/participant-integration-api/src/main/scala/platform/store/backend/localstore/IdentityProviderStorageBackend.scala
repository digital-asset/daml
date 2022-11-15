package com.daml.platform.store.backend.localstore

import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.lf.data.Ref

import java.sql.Connection

trait IdentityProviderStorageBackend {
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
}

object IdentityProviderStorageBackend {


}
