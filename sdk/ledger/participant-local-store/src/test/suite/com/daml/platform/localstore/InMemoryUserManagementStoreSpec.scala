// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.platform.localstore.api.UserManagementStore
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future

class InMemoryUserManagementStoreSpec extends AsyncFreeSpec with UserStoreTests {

  override def newStore(): UserManagementStore =
    new InMemoryUserManagementStore(createAdmin = false)

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig): Future[Unit] =
    Future.unit

}
