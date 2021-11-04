// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.platform.store.DbType
import com.daml.platform.store.backend.h2.H2StorageBackendFactory
import com.daml.platform.store.backend.oracle.OracleStorageBackendFactory
import com.daml.platform.store.backend.postgresql.PostgresStorageBackendFactory

trait StorageBackendFactory {
  def createIngestionStorageBackend: IngestionStorageBackend[_]
  def createParameterStorageBackend: ParameterStorageBackend
  def createConfigurationStorageBackend: ConfigurationStorageBackend
  def createPartyStorageBackend: PartyStorageBackend
  def createPackageStorageBackend: PackageStorageBackend
  def createDeduplicationStorageBackend: DeduplicationStorageBackend
  def createCompletionStorageBackend: CompletionStorageBackend
  def createContractStorageBackend: ContractStorageBackend
  def createEventStorageBackend: EventStorageBackend
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

  def readStorageBackendFor(dbType: DbType): ReadStorageBackend = {
    val factory = of(dbType)
    ReadStorageBackend(
      configurationStorageBackend = factory.createConfigurationStorageBackend,
      partyStorageBackend = factory.createPartyStorageBackend,
      packageStorageBackend = factory.createPackageStorageBackend,
      completionStorageBackend = factory.createCompletionStorageBackend,
      contractStorageBackend = factory.createContractStorageBackend,
      eventStorageBackend = factory.createEventStorageBackend,
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
