// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.api.domain.IdentityProviderConfig
import com.digitalasset.canton.ledger.localstore.InMemoryUserManagementStore
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future

class InMemoryUserManagementStoreSpec extends AsyncFreeSpec with UserStoreTests with BaseTest {

  override def newStore(): UserManagementStore =
    new InMemoryUserManagementStore(
      createAdmin = false,
      loggerFactory = loggerFactory,
    )

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig): Future[Unit] =
    Future.unit

}
