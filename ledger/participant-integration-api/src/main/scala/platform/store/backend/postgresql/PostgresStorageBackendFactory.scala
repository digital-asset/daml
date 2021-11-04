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

object PostgresStorageBackendFactory
    extends StorageBackendFactory
    with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(PGSchema.schema)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new PartyStorageBackendTemplate(PostgresQueryStrategy)

  override val createDeduplicationStorageBackend: DeduplicationStorageBackend =
    PostgresDeduplicationStorageBackend

  override val createCompletionStorageBackend: CompletionStorageBackend =
    new CompletionStorageBackendTemplate(PostgresQueryStrategy)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): ContractStorageBackend =
    new ContractStorageBackendTemplate(PostgresQueryStrategy)

  override def createEventStorageBackend(ledgerEndCache: LedgerEndCache): EventStorageBackend =
    PostgresEventStorageBackend

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    PostgresDataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    PostgresDBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    PostgresResetStorageBackend
}
