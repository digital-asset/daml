// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.oracle

import com.daml.platform.store.backend.common.{
  CompletionStorageBackendTemplate,
  ConfigurationStorageBackendTemplate,
  ContractStorageBackendTemplate,
  IngestionStorageBackendTemplate,
  IntegrityStorageBackendTemplate,
  PackageStorageBackendTemplate,
  ParameterStorageBackendTemplate,
  PartyStorageBackendTemplate,
}
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
}

object OracleStorageBackendFactory extends StorageBackendFactory {
  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(OracleSchema.schema)

  override val createParameterStorageBackend: ParameterStorageBackend =
    ParameterStorageBackendTemplate

  override val createConfigurationStorageBackend: ConfigurationStorageBackend =
    ConfigurationStorageBackendTemplate

  override val createPartyStorageBackend: PartyStorageBackend =
    new PartyStorageBackendTemplate(OracleQueryStrategy)

  override val createPackageStorageBackend: PackageStorageBackend =
    PackageStorageBackendTemplate

  override val createDeduplicationStorageBackend: DeduplicationStorageBackend =
    OracleDeduplicationStorageBackend

  override val createCompletionStorageBackend: CompletionStorageBackend =
    new CompletionStorageBackendTemplate(OracleQueryStrategy)

  override val createContractStorageBackend: ContractStorageBackend =
    new ContractStorageBackendTemplate(OracleQueryStrategy)

  override val createEventStorageBackend: EventStorageBackend =
    OracleEventStorageBackend

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    OracleDataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    OracleDBLockStorageBackend

  override val createIntegrityStorageBackend: IntegrityStorageBackend =
    IntegrityStorageBackendTemplate

  override val createResetStorageBackend: ResetStorageBackend =
    OracleResetStorageBackend
}
