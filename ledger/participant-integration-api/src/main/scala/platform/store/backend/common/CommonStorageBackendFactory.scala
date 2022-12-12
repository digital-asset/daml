// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import com.daml.platform.store.backend._
import com.daml.platform.store.backend.localstore.{
  IdentityProviderStorageBackend,
  IdentityProviderStorageBackendImpl,
  PartyRecordStorageBackend,
  PartyRecordStorageBackendImpl,
  UserManagementStorageBackend,
  UserManagementStorageBackendImpl,
}
import com.daml.platform.store.cache.LedgerEndCache

trait CommonStorageBackendFactory extends StorageBackendFactory {

  override val createParameterStorageBackend: ParameterStorageBackend =
    ParameterStorageBackendImpl

  override val createMeteringParameterStorageBackend: MeteringParameterStorageBackend =
    MeteringParameterStorageBackendImpl

  override val createIntegrityStorageBackend: IntegrityStorageBackend =
    IntegrityStorageBackendImpl

  override val createStringInterningStorageBackend: StringInterningStorageBackend =
    StringInterningStorageBackendImpl

  override val createUserManagementStorageBackend: UserManagementStorageBackend =
    UserManagementStorageBackendImpl

  override val createIdentityProviderConfigStorageBackend: IdentityProviderStorageBackend =
    IdentityProviderStorageBackendImpl

  override def createMeteringStorageReadBackend(
      ledgerEndCache: LedgerEndCache
  ): MeteringStorageReadBackend =
    MeteringStorageBackendReadTemplate

  def createMeteringStorageWriteBackend: MeteringStorageWriteBackend = {
    MeteringStorageBackendWriteTemplate
  }

  override def createPartyRecordStorageBackend: PartyRecordStorageBackend =
    PartyRecordStorageBackendImpl

}
