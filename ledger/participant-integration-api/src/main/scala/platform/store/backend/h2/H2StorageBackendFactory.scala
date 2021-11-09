// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import com.daml.platform.store.backend.common.{
  CommonStorageBackendFactory,
  CompletionStorageBackendTemplate,
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

object H2StorageBackendFactory extends StorageBackendFactory with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(H2Schema.schema)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new PartyStorageBackendTemplate(H2QueryStrategy, ledgerEndCache)

  override val createDeduplicationStorageBackend: DeduplicationStorageBackend =
    H2DeduplicationStorageBackend

  override val createCompletionStorageBackend: CompletionStorageBackend =
    new CompletionStorageBackendTemplate(H2QueryStrategy)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): ContractStorageBackend =
    new H2ContractStorageBackend(ledgerEndCache)

  override def createEventStorageBackend(ledgerEndCache: LedgerEndCache): EventStorageBackend =
    new H2EventStorageBackend(ledgerEndCache)

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    H2DataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    H2DBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    H2ResetStorageBackend
}
