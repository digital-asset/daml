// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.common

import com.digitalasset.canton.platform.store.backend.*
import com.digitalasset.canton.platform.store.backend.localstore.{
  IdentityProviderStorageBackend,
  IdentityProviderStorageBackendImpl,
  PartyRecordStorageBackend,
  PartyRecordStorageBackendImpl,
  UserManagementStorageBackend,
  UserManagementStorageBackendImpl,
}

trait CommonStorageBackendFactory extends StorageBackendFactory {

  override val createIntegrityStorageBackend: IntegrityStorageBackend =
    IntegrityStorageBackendImpl

  override val createStringInterningStorageBackend: StringInterningStorageBackend =
    StringInterningStorageBackendImpl

  override val createUserManagementStorageBackend: UserManagementStorageBackend =
    UserManagementStorageBackendImpl

  override val createIdentityProviderConfigStorageBackend: IdentityProviderStorageBackend =
    IdentityProviderStorageBackendImpl

  override def createPartyRecordStorageBackend: PartyRecordStorageBackend =
    PartyRecordStorageBackendImpl

}
