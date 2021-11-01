// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.postgresql

import com.daml.platform.store.backend.common.{
  CompletionStorageBackendTemplate,
  ConfigurationStorageBackendTemplate,
  ContractStorageBackendTemplate,
  IngestionStorageBackendTemplate,
  IntegrityStorageBackendTemplate,
  PackageStorageBackendTemplate,
  ParameterStorageBackendTemplate,
  PartyStorageBackendTemplate,
  StringInterningStorageBackendTemplate,
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
  StringInterningStorageBackend,
}

object PostgresStorageBackendFactory extends StorageBackendFactory {
  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(PGSchema.schema)

  override val createParameterStorageBackend: ParameterStorageBackend =
    ParameterStorageBackendTemplate

  override val createConfigurationStorageBackend: ConfigurationStorageBackend =
    ConfigurationStorageBackendTemplate

  override val createPartyStorageBackend: PartyStorageBackend =
    new PartyStorageBackendTemplate(PostgresQueryStrategy)

  override val createPackageStorageBackend: PackageStorageBackend =
    PackageStorageBackendTemplate

  override val createDeduplicationStorageBackend: DeduplicationStorageBackend =
    PostgresDeduplicationStorageBackend

  override val createCompletionStorageBackend: CompletionStorageBackend =
    new CompletionStorageBackendTemplate(PostgresQueryStrategy)

  override val createContractStorageBackend: ContractStorageBackend =
    new ContractStorageBackendTemplate(PostgresQueryStrategy)

  override val createEventStorageBackend: EventStorageBackend =
    PostgresEventStorageBackend

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    PostgresDataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    PostgresDBLockStorageBackend

  override val createIntegrityStorageBackend: IntegrityStorageBackend =
    IntegrityStorageBackendTemplate

  override val createResetStorageBackend: ResetStorageBackend =
    PostgresResetStorageBackend

  override val createStringInterningStorageBackend: StringInterningStorageBackend =
    StringInterningStorageBackendTemplate
}
