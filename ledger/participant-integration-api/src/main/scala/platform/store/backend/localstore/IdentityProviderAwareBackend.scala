// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import anorm.SqlStringInterpolation
import com.daml.ledger.api.domain.IdentityProviderId

import java.sql.Connection

object IdentityProviderAwareBackend {

  def updateIdentityProviderId(
      tableName: String
  )(internalId: Int, identityProviderId: Option[IdentityProviderId.Id])(
      connection: Connection
  ): Boolean = {
    val rowsUpdated =
      SQL"""
         UPDATE #$tableName
         SET identity_provider_id = ${identityProviderId.map(_.value): Option[String]}
         WHERE
             internal_id = ${internalId}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }

}
