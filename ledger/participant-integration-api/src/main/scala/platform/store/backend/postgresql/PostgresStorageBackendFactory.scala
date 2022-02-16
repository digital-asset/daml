// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.common._
import com.daml.platform.store.backend._
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

object PostgresStorageBackendFactory
    extends StorageBackendFactory
    with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(PGSchema.schema)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend =
    new ContractStorageBackendTemplate(queryStrategy, ledgerEndCache, stringInterning)

  override def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): EventStorageBackend =
    new PostgresEventStorageBackend(ledgerEndCache, stringInterning)

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    PostgresDataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    PostgresDBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    PostgresResetStorageBackend

  override protected def queryStrategy: QueryStrategy = PostgresQueryStrategy
}
