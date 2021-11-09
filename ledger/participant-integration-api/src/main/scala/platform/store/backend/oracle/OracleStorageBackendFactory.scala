// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

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

object OracleStorageBackendFactory extends StorageBackendFactory with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(OracleSchema.schema)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new PartyStorageBackendTemplate(OracleQueryStrategy, ledgerEndCache)

  override val createDeduplicationStorageBackend: DeduplicationStorageBackend =
    OracleDeduplicationStorageBackend

  override def createCompletionStorageBackend: CompletionStorageBackend =
    new CompletionStorageBackendTemplate(OracleQueryStrategy)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): ContractStorageBackend =
    new ContractStorageBackendTemplate(OracleQueryStrategy, ledgerEndCache)

  override def createEventStorageBackend(ledgerEndCache: LedgerEndCache): EventStorageBackend =
    new OracleEventStorageBackend(ledgerEndCache)

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    OracleDataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    OracleDBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    OracleResetStorageBackend
}
