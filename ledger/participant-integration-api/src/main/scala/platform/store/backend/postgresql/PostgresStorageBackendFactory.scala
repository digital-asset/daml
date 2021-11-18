// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.common.{
  CommonStorageBackendFactory,
  CompletionStorageBackendTemplate,
  ContractStorageBackendTemplate,
  IngestionStorageBackendTemplate,
  PartyStorageBackendTemplate,
}
import com.daml.platform.store.backend.{
  CompletionStorageBackend,
  ContractStorageBackend,
  DBLockStorageBackend,
  DataSourceStorageBackend,
  DeduplicationStorageBackend,
  EventStorageBackend,
  IngestionStorageBackend,
  PartyStorageBackend,
  ResetStorageBackend,
  StorageBackendFactory,
}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

object PostgresStorageBackendFactory
    extends StorageBackendFactory
    with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(PGSchema.schema)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new PartyStorageBackendTemplate(PostgresQueryStrategy, ledgerEndCache)

  override val createDeduplicationStorageBackend: DeduplicationStorageBackend =
    PostgresDeduplicationStorageBackend

  override def createCompletionStorageBackend(
      stringInterning: StringInterning
  ): CompletionStorageBackend =
    new CompletionStorageBackendTemplate(PostgresQueryStrategy, stringInterning)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend =
    new ContractStorageBackendTemplate(PostgresQueryStrategy, ledgerEndCache, stringInterning)

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
}
