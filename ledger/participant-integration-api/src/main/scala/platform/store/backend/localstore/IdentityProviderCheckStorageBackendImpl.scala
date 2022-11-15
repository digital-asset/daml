// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.backend.localstore

import anorm.RowParser
import anorm.SqlParser.int
import com.daml.lf.data.Ref.IdentityProviderId
import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._

import java.sql.Connection

object IdentityProviderCheckStorageBackendImpl extends IdentityProviderCheckStorageBackend {
  private val IntParser0: RowParser[Int] =
    int("dummy") map { i => i }

  def idpConfigByIdExists(id: IdentityProviderId.Id)(connection: Connection): Boolean = {
    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val res: Seq[_] =
      SQL"""
         SELECT 1 AS dummy
         FROM participant_identity_provider_config idpcfg
         WHERE idpcfg.identity_provider_id = ${id.value: String}
         """.asVectorOf(IntParser0)(connection)
    assert(res.length <= 1)
    res.length == 1
  }

}
