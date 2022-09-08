// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import com.daml.ledger.participant.state.index.impl.inmemory.InMemoryUserManagementStore
import com.daml.platform.store.platform.usermanagement.UserStoreTests
import org.scalatest.freespec.AsyncFreeSpec

class InMemoryUserManagementStoreSpec extends AsyncFreeSpec with UserStoreTests {

  override def newStore() = new InMemoryUserManagementStore(createAdmin = false)

}
