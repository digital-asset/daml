// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import com.daml.platform.store.backend.common.{
  CommonStorageBackendFactory,
  ContractStorageBackendTemplate,
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

object OracleStorageBackendFactory extends StorageBackendFactory with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(OracleSchema.schema)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend =
    new ContractStorageBackendTemplate(queryStrategy, ledgerEndCache, stringInterning)

  override def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): EventStorageBackend =
    new OracleEventStorageBackend(ledgerEndCache, stringInterning)

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    OracleDataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    OracleDBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    OracleResetStorageBackend

  override protected def queryStrategy: QueryStrategy = OracleQueryStrategy
}
