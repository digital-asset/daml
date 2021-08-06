// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.platform.store.backend.common.AppendOnlySchema
import com.daml.platform.store.backend.h2.H2StorageBackend
import com.daml.platform.store.backend.oracle.OracleStorageBackend
import com.daml.platform.store.backend.postgresql.PostgresStorageBackend
import com.daml.testing.oracle.OracleAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.Suite

/** Creates a database and a [[StorageBackend]].
  * Used by [[StorageBackendSpec]] to run all StorageBackend tests on different databases.
  */
private[backend] trait StorageBackendProvider[DB_BATCH] {
  protected def jdbcUrl: String
  protected def backend: StorageBackend[DB_BATCH]
}

private[backend] trait StorageBackendProviderPostgres
    extends StorageBackendProvider[AppendOnlySchema.Batch]
    with PostgresAroundAll { this: Suite =>
  override protected def jdbcUrl: String = postgresDatabase.url
  override protected val backend: StorageBackend[AppendOnlySchema.Batch] = PostgresStorageBackend
}
private[backend] object StorageBackendProviderPostgres {
  type DB_BATCH = AppendOnlySchema.Batch
}

private[backend] trait StorageBackendProviderH2
    extends StorageBackendProvider[AppendOnlySchema.Batch] { this: Suite =>
  override protected def jdbcUrl: String = "jdbc:h2:mem:storage_backend_provider;db_close_delay=-1"
  override protected val backend: StorageBackend[AppendOnlySchema.Batch] = H2StorageBackend
}
private[backend] object StorageBackendProviderH2 {
  type DB_BATCH = AppendOnlySchema.Batch
}

private[backend] trait StorageBackendProviderOracle
    extends StorageBackendProvider[AppendOnlySchema.Batch]
    with OracleAroundAll { this: Suite =>
  override protected def jdbcUrl: String =
    s"jdbc:oracle:thin:$oracleUser/$oraclePwd@localhost:$oraclePort/ORCLPDB1"
  override protected val backend: StorageBackend[AppendOnlySchema.Batch] = OracleStorageBackend
}
private[backend] object StorageBackendProviderOracle {
  type DB_BATCH = AppendOnlySchema.Batch
}
