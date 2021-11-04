// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import com.daml.platform.store.backend.{
  ConfigurationStorageBackend,
  IntegrityStorageBackend,
  PackageStorageBackend,
  ParameterStorageBackend,
  StorageBackendFactory,
  StringInterningStorageBackend,
}
import com.daml.platform.store.cache.LedgerEndCache

trait CommonStorageBackendFactory extends StorageBackendFactory {

  override def createPackageStorageBackend(ledgerEndCache: LedgerEndCache): PackageStorageBackend =
    PackageStorageBackendTemplate

  override val createParameterStorageBackend: ParameterStorageBackend =
    ParameterStorageBackendTemplate

  override def createConfigurationStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): ConfigurationStorageBackend =
    ConfigurationStorageBackendTemplate

  override val createIntegrityStorageBackend: IntegrityStorageBackend =
    IntegrityStorageBackendTemplate

  override val createStringInterningStorageBackend: StringInterningStorageBackend =
    StringInterningStorageBackendTemplate
}
