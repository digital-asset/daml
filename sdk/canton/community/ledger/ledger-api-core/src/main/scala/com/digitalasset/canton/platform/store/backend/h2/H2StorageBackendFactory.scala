// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.h2

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.common.{
  CommonStorageBackendFactory,
  CompletionStorageBackendTemplate,
  ContractStorageBackendTemplate,
  IngestionStorageBackendTemplate,
  PartyStorageBackendTemplate,
}
import com.digitalasset.canton.platform.store.backend.localstore.{
  PartyRecordStorageBackend,
  PartyRecordStorageBackendImpl,
}
import com.digitalasset.canton.platform.store.backend.{
  CompletionStorageBackend,
  ContractStorageBackend,
  DBLockStorageBackend,
  DataSourceStorageBackend,
  EventStorageBackend,
  IngestionStorageBackend,
  PartyStorageBackend,
  ResetStorageBackend,
  StorageBackendFactory,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

object H2StorageBackendFactory extends StorageBackendFactory with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(H2QueryStrategy, H2Schema.schema)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new PartyStorageBackendTemplate(H2QueryStrategy, ledgerEndCache)

  override def createPartyRecordStorageBackend: PartyRecordStorageBackend =
    PartyRecordStorageBackendImpl

  override def createCompletionStorageBackend(
      stringInterning: StringInterning,
      loggerFactory: NamedLoggerFactory,
  ): CompletionStorageBackend =
    new CompletionStorageBackendTemplate(H2QueryStrategy, stringInterning, loggerFactory)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend =
    new ContractStorageBackendTemplate(H2QueryStrategy, ledgerEndCache, stringInterning)

  override def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      loggerFactory: NamedLoggerFactory,
  ): EventStorageBackend =
    new H2EventStorageBackend(
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      loggerFactory = loggerFactory,
    )

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    H2DataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    H2DBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    H2ResetStorageBackend
}
