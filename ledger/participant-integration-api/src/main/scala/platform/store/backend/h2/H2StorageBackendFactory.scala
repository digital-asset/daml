// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import com.daml.platform.store.backend.common.{
  CommonStorageBackendFactory,
  IngestionStorageBackendTemplate,
  QueryStrategy,
}
import com.daml.platform.store.backend.{
  ContractStorageBackend,
  DBLockStorageBackend,
  DataSourceStorageBackend,
  EventStorageBackend,
  IngestionStorageBackend,
  ResetStorageBackend,
  StorageBackendFactory,
}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

object H2StorageBackendFactory extends StorageBackendFactory with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(H2Schema.schema)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend =
    new H2ContractStorageBackend(ledgerEndCache, stringInterning)

  override def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): EventStorageBackend =
    new H2EventStorageBackend(ledgerEndCache, stringInterning)

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    H2DataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    H2DBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    H2ResetStorageBackend

  override protected def queryStrategy: QueryStrategy = H2QueryStrategy
}
