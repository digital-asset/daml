// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import com.daml.platform.store.backend.common.{
  CompletionStorageBackendTemplate,
  ConfigurationStorageBackendTemplate,
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

object H2StorageBackendFactory extends StorageBackendFactory {
  override val createIngestionStorageBackend: IngestionStorageBackend[_] =
    new IngestionStorageBackendTemplate(H2Schema.schema)

  override val createParameterStorageBackend: ParameterStorageBackend =
    ParameterStorageBackendTemplate

  override val createConfigurationStorageBackend: ConfigurationStorageBackend =
    ConfigurationStorageBackendTemplate

  override val createPartyStorageBackend: PartyStorageBackend =
    new PartyStorageBackendTemplate(H2QueryStrategy)

  override val createPackageStorageBackend: PackageStorageBackend =
    PackageStorageBackendTemplate

  override val createDeduplicationStorageBackend: DeduplicationStorageBackend =
    H2DeduplicationStorageBackend

  override val createCompletionStorageBackend: CompletionStorageBackend =
    new CompletionStorageBackendTemplate(H2QueryStrategy)

  override val createContractStorageBackend: ContractStorageBackend =
    H2ContractStorageBackend

  override val createEventStorageBackend: EventStorageBackend =
    H2EventStorageBackend

  override val createDataSourceStorageBackend: DataSourceStorageBackend =
    H2DataSourceStorageBackend

  override val createDBLockStorageBackend: DBLockStorageBackend =
    H2DBLockStorageBackend

  override val createIntegrityStorageBackend: IntegrityStorageBackend =
    IntegrityStorageBackendTemplate

  override val createResetStorageBackend: ResetStorageBackend =
    H2ResetStorageBackend

  override val createStringInterningStorageBackend: StringInterningStorageBackend =
    StringInterningStorageBackendTemplate
}
