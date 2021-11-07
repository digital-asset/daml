// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import com.daml.platform.store.backend.{
  CompletionStorageBackend,
  ConfigurationStorageBackend,
  ContractStorageBackend,
  DBLockStorageBackend,
  DataSourceStorageBackend,
  DeduplicationStorageBackend,
  EventStorageBackend,
  IngestionStorageBackend,
  IntegrityStorageBackend,
  PackageStorageBackend,
  ParameterStorageBackend,
  PartyStorageBackend,
  ResetStorageBackend,
  StorageBackendFactory,
  StringInterningStorageBackend,
}
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

object MStorageBackendFactory extends StorageBackendFactory {
  override def createIngestionStorageBackend: IngestionStorageBackend[_] =
    MIngestionStorageBackend

  override def createParameterStorageBackend: ParameterStorageBackend =
    MParameterStorageBackend

  override def createConfigurationStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): ConfigurationStorageBackend =
    new MConfigurationStorageBackend(ledgerEndCache)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new MPartyStorageBackend(ledgerEndCache)

  override def createPackageStorageBackend(ledgerEndCache: LedgerEndCache): PackageStorageBackend =
    new MPackageStorageBackend(ledgerEndCache)

  override def createDeduplicationStorageBackend: DeduplicationStorageBackend =
    MDeduplicationStorageBackend

  override def createCompletionStorageBackend(
      stringInterning: StringInterning
  ): CompletionStorageBackend =
    MCompletionStorageBackend

  override def createContractStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): ContractStorageBackend =
    new MContractStorageBackend(ledgerEndCache)

  override def createEventStorageBackend(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): EventStorageBackend =
    new MEventStorageBackend(ledgerEndCache)

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    MDataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    MDBLockStorageBackend

  override def createIntegrityStorageBackend: IntegrityStorageBackend =
    MIntegrityStorageBackend

  override def createResetStorageBackend: ResetStorageBackend =
    MResetStorageBackend

  override def createStringInterningStorageBackend: StringInterningStorageBackend =
    MStringInterningStorageBackend
}
