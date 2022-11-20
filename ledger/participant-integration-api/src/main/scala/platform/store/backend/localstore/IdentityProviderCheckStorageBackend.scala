// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.platform.store.backend.localstore

import com.daml.ledger.api.domain.IdentityProviderId

import java.sql.Connection

trait IdentityProviderCheckStorageBackend {

  def idpConfigByIdExists(id: IdentityProviderId.Id)(connection: Connection): Boolean

}
