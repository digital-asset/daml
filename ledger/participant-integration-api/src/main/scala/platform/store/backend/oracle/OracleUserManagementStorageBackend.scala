// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import com.daml.platform.store.backend.common.{
  ComposableQuery,
  UserManagementStorageBackendTemplate,
}

object OracleUserManagementStorageBackend extends UserManagementStorageBackendTemplate {

  override protected def limitClause(number: Int): ComposableQuery.CompositeSql = {
    import com.daml.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    cSQL"FETCH FIRST $number ROWS ONLY"
  }

}
