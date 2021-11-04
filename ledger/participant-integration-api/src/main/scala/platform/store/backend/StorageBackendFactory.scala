// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.platform.store.DbType
import com.daml.platform.store.backend.h2.H2StorageBackendFactory
import com.daml.platform.store.backend.oracle.OracleStorageBackendFactory
import com.daml.platform.store.backend.postgresql.PostgresStorageBackendFactory
import com.daml.platform.store.cache.LedgerEndCache

trait StorageBackendFactory {
  def createIngestionStorageBackend: IngestionStorageBackend[_]
  def createParameterStorageBackend: ParameterStorageBackend
  def createConfigurationStorageBackend(ledgerEndCache: LedgerEndCache): ConfigurationStorageBackend
  def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend
  def createPackageStorageBackend(ledgerEndCache: LedgerEndCache): PackageStorageBackend
  def createDeduplicationStorageBackend: DeduplicationStorageBackend
  def createCompletionStorageBackend: CompletionStorageBackend
  def createContractStorageBackend(ledgerEndCache: LedgerEndCache): ContractStorageBackend
  def createEventStorageBackend(ledgerEndCache: LedgerEndCache): EventStorageBackend
  def createDataSourceStorageBackend: DataSourceStorageBackend
  def createDBLockStorageBackend: DBLockStorageBackend
  def createIntegrityStorageBackend: IntegrityStorageBackend
  def createResetStorageBackend: ResetStorageBackend
  def createStringInterningStorageBackend: StringInterningStorageBackend
}

object StorageBackendFactory {
  def of(dbType: DbType): StorageBackendFactory =
    dbType match {
      case DbType.H2Database => H2StorageBackendFactory
      case DbType.Postgres => PostgresStorageBackendFactory
      case DbType.Oracle => OracleStorageBackendFactory
    }

  def readStorageBackendFor(
      dbType: DbType,
      ledgerEndCache: LedgerEndCache,
  ): ReadStorageBackend = {
    val factory = of(dbType)
    ReadStorageBackend(
      configurationStorageBackend = factory.createConfigurationStorageBackend(ledgerEndCache),
      partyStorageBackend = factory.createPartyStorageBackend(ledgerEndCache),
      packageStorageBackend = factory.createPackageStorageBackend(ledgerEndCache),
      completionStorageBackend = factory.createCompletionStorageBackend,
      contractStorageBackend = factory.createContractStorageBackend(ledgerEndCache),
      eventStorageBackend = factory.createEventStorageBackend(ledgerEndCache),
    )
  }
}

case class ReadStorageBackend(
    configurationStorageBackend: ConfigurationStorageBackend,
    partyStorageBackend: PartyStorageBackend,
    packageStorageBackend: PackageStorageBackend,
    completionStorageBackend: CompletionStorageBackend,
    contractStorageBackend: ContractStorageBackend,
    eventStorageBackend: EventStorageBackend,
)
