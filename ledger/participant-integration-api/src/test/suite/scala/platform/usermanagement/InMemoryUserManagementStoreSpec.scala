// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.usermanagement

import com.daml.ledger.participant.state.index.impl.inmemory.InMemoryUserManagementStore
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.platform.store.platform.usermanagement.UserManagementStoreSpecBase
import org.scalatest.Assertion
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future

class InMemoryUserManagementStoreSpec extends AsyncFreeSpec with UserManagementStoreSpecBase {

  override def testIt(f: UserManagementStore => Future[Assertion]): Future[Assertion] = {
    f(
      new InMemoryUserManagementStore(
        createAdmin = false
      )
    )
  }

}
