// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.platform.store.DbType
import com.digitalasset.canton.platform.store.backend.h2.H2StorageBackendFactory
import com.digitalasset.canton.platform.store.backend.localstore.{
  IdentityProviderStorageBackend,
  PartyRecordStorageBackend,
  UserManagementStorageBackend,
}
import com.digitalasset.canton.platform.store.backend.oracle.OracleStorageBackendFactory
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresStorageBackendFactory
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.interning.StringInterning

trait StorageBackendFactory {
  def createIngestionStorageBackend: IngestionStorageBackend[_]
  def createParameterStorageBackend: ParameterStorageBackend
  def createMeteringParameterStorageBackend: MeteringParameterStorageBackend
  def createConfigurationStorageBackend(ledgerEndCache: LedgerEndCache): ConfigurationStorageBackend
  def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend
  def createPartyRecordStorageBackend: PartyRecordStorageBackend
  def createPackageStorageBackend(ledgerEndCache: LedgerEndCache): PackageStorageBackend
  def createCompletionStorageBackend(
      stringInterning: StringInterning,
      loggerFactory: NamedLoggerFactory,
  ): CompletionStorageBackend
  def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend
  def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      loggerFactory: NamedLoggerFactory,
  ): EventStorageBackend
  def createDataSourceStorageBackend: DataSourceStorageBackend
  def createDBLockStorageBackend: DBLockStorageBackend
  def createIntegrityStorageBackend: IntegrityStorageBackend
  def createResetStorageBackend: ResetStorageBackend
  def createStringInterningStorageBackend: StringInterningStorageBackend
  def createUserManagementStorageBackend: UserManagementStorageBackend
  def createIdentityProviderConfigStorageBackend: IdentityProviderStorageBackend
  def createMeteringStorageReadBackend(ledgerEndCache: LedgerEndCache): MeteringStorageReadBackend
  def createMeteringStorageWriteBackend: MeteringStorageWriteBackend

  final def readStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      loggerFactory: NamedLoggerFactory,
  ): ReadStorageBackend =
    ReadStorageBackend(
      configurationStorageBackend = createConfigurationStorageBackend(ledgerEndCache),
      partyStorageBackend = createPartyStorageBackend(ledgerEndCache),
      packageStorageBackend = createPackageStorageBackend(ledgerEndCache),
      completionStorageBackend = createCompletionStorageBackend(stringInterning, loggerFactory),
      contractStorageBackend = createContractStorageBackend(ledgerEndCache, stringInterning),
      eventStorageBackend =
        createEventStorageBackend(ledgerEndCache, stringInterning, loggerFactory),
      meteringStorageBackend = createMeteringStorageReadBackend(ledgerEndCache),
    )
}

object StorageBackendFactory {
  def of(dbType: DbType, loggerFactory: NamedLoggerFactory): StorageBackendFactory =
    dbType match {
      case DbType.H2Database => H2StorageBackendFactory
      case DbType.Postgres => PostgresStorageBackendFactory(loggerFactory)
      case DbType.Oracle => OracleStorageBackendFactory
    }
}

final case class ReadStorageBackend(
    configurationStorageBackend: ConfigurationStorageBackend,
    partyStorageBackend: PartyStorageBackend,
    packageStorageBackend: PackageStorageBackend,
    completionStorageBackend: CompletionStorageBackend,
    contractStorageBackend: ContractStorageBackend,
    eventStorageBackend: EventStorageBackend,
    meteringStorageBackend: MeteringStorageReadBackend,
)
