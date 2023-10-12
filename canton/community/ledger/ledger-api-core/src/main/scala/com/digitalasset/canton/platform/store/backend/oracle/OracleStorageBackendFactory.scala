// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.oracle

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.backend.common.{
  CommonStorageBackendFactory,
  CompletionStorageBackendTemplate,
  ConfigurationStorageBackendTemplate,
  ContractStorageBackendTemplate,
  IngestionStorageBackendTemplate,
  PackageStorageBackendTemplate,
  PartyStorageBackendTemplate,
}
import com.digitalasset.canton.platform.store.backend.{
  CompletionStorageBackend,
  ConfigurationStorageBackend,
  ContractStorageBackend,
  DBLockStorageBackend,
  DataSourceStorageBackend,
  EventStorageBackend,
  IngestionStorageBackend,
  PackageStorageBackend,
  PartyStorageBackend,
  ResetStorageBackend,
  StorageBackendFactory,
}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

object OracleStorageBackendFactory extends StorageBackendFactory with CommonStorageBackendFactory {

  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(OracleQueryStrategy, OracleSchema.schema)

  override def createPackageStorageBackend(ledgerEndCache: LedgerEndCache): PackageStorageBackend =
    new PackageStorageBackendTemplate(OracleQueryStrategy, ledgerEndCache)

  override def createConfigurationStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): ConfigurationStorageBackend =
    new ConfigurationStorageBackendTemplate(OracleQueryStrategy, ledgerEndCache)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new PartyStorageBackendTemplate(OracleQueryStrategy, ledgerEndCache)

  override def createCompletionStorageBackend(
      stringInterning: StringInterning,
      loggerFactory: NamedLoggerFactory,
  ): CompletionStorageBackend =
    new CompletionStorageBackendTemplate(OracleQueryStrategy, stringInterning, loggerFactory)

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend =
    new ContractStorageBackendTemplate(OracleQueryStrategy, ledgerEndCache, stringInterning)

  override def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      loggerFactory: NamedLoggerFactory,
  ): EventStorageBackend =
    new OracleEventStorageBackend(
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
      loggerFactory = loggerFactory,
    )

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    OracleDataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    OracleDBLockStorageBackend

  override val createResetStorageBackend: ResetStorageBackend =
    OracleResetStorageBackend

}
