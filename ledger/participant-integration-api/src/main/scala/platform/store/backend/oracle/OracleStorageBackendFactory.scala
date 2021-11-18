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
import com.daml.platform.store.interning.StringInterning

object OracleStorageBackendFactory extends StorageBackendFactory with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(OracleSchema.schema)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new PartyStorageBackendTemplate(OracleQueryStrategy, ledgerEndCache)

  override val createDeduplicationStorageBackend: DeduplicationStorageBackend =
    OracleDeduplicationStorageBackend

  override def createCompletionStorageBackend(
      stringInterning: StringInterning
  ): CompletionStorageBackend =
    new CompletionStorageBackendTemplate(OracleQueryStrategy, stringInterning)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend =
    new ContractStorageBackendTemplate(OracleQueryStrategy, ledgerEndCache, stringInterning)

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
}
