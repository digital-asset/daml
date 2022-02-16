// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import com.daml.platform.store.backend._
import com.daml.platform.store.cache.LedgerEndCache
import com.daml.platform.store.interning.StringInterning

trait CommonStorageBackendFactory extends StorageBackendFactory {

  protected def queryStrategy: QueryStrategy

  override def createCompletionStorageBackend(
      stringInterning: StringInterning
  ): CompletionStorageBackend =
    new CompletionStorageBackendTemplate(queryStrategy, stringInterning)

  override def createPartyStorageBackend(ledgerEndCache: LedgerEndCache): PartyStorageBackend =
    new PartyStorageBackendTemplate(queryStrategy, ledgerEndCache)

  override def createPackageStorageBackend(ledgerEndCache: LedgerEndCache): PackageStorageBackend =
    new PackageStorageBackendTemplate(ledgerEndCache)

  override val createParameterStorageBackend: ParameterStorageBackend =
    ParameterStorageBackendTemplate

  override val createMeteringParameterStorageBackend: MeteringParameterStorageBackend =
    MeteringParameterStorageBackendTemplate

  override def createConfigurationStorageBackend(
      ledgerEndCache: LedgerEndCache
  ): ConfigurationStorageBackend =
    new ConfigurationStorageBackendTemplate(ledgerEndCache)

  override val createIntegrityStorageBackend: IntegrityStorageBackend =
    IntegrityStorageBackendTemplate

  override val createStringInterningStorageBackend: StringInterningStorageBackend =
    StringInterningStorageBackendTemplate

  override val createUserManagementStorageBackend: UserManagementStorageBackend =
    UserManagementStorageBackendTemplate

  override def createMeteringStorageReadBackend(
      ledgerEndCache: LedgerEndCache
  ): MeteringStorageReadBackend =
    new MeteringStorageBackendReadTemplate(ledgerEndCache)

  def createMeteringStorageWriteBackend: MeteringStorageWriteBackend = {
    MeteringStorageBackendWriteTemplate
  }

}
