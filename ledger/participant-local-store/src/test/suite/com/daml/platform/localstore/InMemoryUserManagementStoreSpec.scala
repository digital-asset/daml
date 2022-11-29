// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.platform.localstore.UserStoreSpecBase.StoreContainer
import org.scalatest.freespec.AsyncFreeSpec

class InMemoryUserManagementStoreSpec extends AsyncFreeSpec with UserStoreTests {

  override def newStore(): StoreContainer = StoreContainer(
    new InMemoryUserManagementStore(createAdmin = false),
    new InMemoryIdentityProviderConfigStore(),
  )

}
