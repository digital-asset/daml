// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import com.daml.platform.store.backend._
import com.daml.platform.store.cache.LedgerEndCache

trait CommonStorageBackendFactory extends StorageBackendFactory {

  override def createPackageStorageBackend(ledgerEndCache: LedgerEndCache): PackageStorageBackend =
    new PackageStorageBackendTemplate(ledgerEndCache)

  override val createParameterStorageBackend: ParameterStorageBackend =
    ParameterStorageBackendTemplate

  override def createConfigurationStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): ConfigurationStorageBackend =
    new ConfigurationStorageBackendTemplate(ledgerEndCache)

  override val createIntegrityStorageBackend: IntegrityStorageBackend =
    IntegrityStorageBackendTemplate

  override val createStringInterningStorageBackend: StringInterningStorageBackend =
    StringInterningStorageBackendTemplate

  override val createUserManagementStorageBackend: UserManagementStorageBackend =
    DefaultUserManagementStorageBackend

  override def createMeteringStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): MeteringStorageBackend =
    new MeteringStorageBackendTemplate(ledgerEndCache)
}
