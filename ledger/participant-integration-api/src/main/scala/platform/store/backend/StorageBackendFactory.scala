// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.platform.store.DbType
import com.daml.platform.store.backend.h2.H2StorageBackendFactory
import com.daml.platform.store.backend.m.MStorageBackendFactory
import com.daml.platform.store.backend.oracle.OracleStorageBackendFactory
import com.daml.platform.store.backend.postgresql.PostgresStorageBackendFactory
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

trait StorageBackendFactory {
  def createIngestionStorageBackend: IngestionStorageBackend[_]
  def createParameterStorageBackend: ParameterStorageBackend
  def createConfigurationStorageBackend(ledgerEndCache: LedgerEndCache): ConfigurationStorageBackend
  def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend
  def createPackageStorageBackend(ledgerEndCache: LedgerEndCache): PackageStorageBackend
  def createDeduplicationStorageBackend: DeduplicationStorageBackend
  def createCompletionStorageBackend(stringInterning: StringInterning): CompletionStorageBackend
  def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend
  def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): EventStorageBackend
  def createDataSourceStorageBackend: DataSourceStorageBackend
  def createDBLockStorageBackend: DBLockStorageBackend
  def createIntegrityStorageBackend: IntegrityStorageBackend
  def createResetStorageBackend: ResetStorageBackend
  def createStringInterningStorageBackend: StringInterningStorageBackend

  final def readStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ReadStorageBackend =
    ReadStorageBackend(
      configurationStorageBackend = createConfigurationStorageBackend(ledgerEndCache),
      partyStorageBackend = createPartyStorageBackend(ledgerEndCache),
      packageStorageBackend = createPackageStorageBackend(ledgerEndCache),
      completionStorageBackend = createCompletionStorageBackend(stringInterning),
      contractStorageBackend = createContractStorageBackend(ledgerEndCache, stringInterning),
      eventStorageBackend = createEventStorageBackend(ledgerEndCache, stringInterning),
    )
}

object StorageBackendFactory {
  def of(dbType: DbType): StorageBackendFactory =
    dbType match {
      case DbType.H2Database => H2StorageBackendFactory
      case DbType.Postgres => PostgresStorageBackendFactory
      case DbType.Oracle => OracleStorageBackendFactory
      case DbType.M => MStorageBackendFactory
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
