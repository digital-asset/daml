// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.platform.usermanagement

import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.resources.TestResourceContext
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, AsyncTestSuite, EitherValues, OptionValues}

import scala.concurrent.Future

trait UserStoreSpecBase
    extends TestResourceContext
    with Matchers
    with OptionValues
    with EitherValues { self: AsyncTestSuite =>

  def newStore(): UserManagementStore

  final protected def testIt(f: UserManagementStore => Future[Assertion]): Future[Assertion] = f(
    newStore()
  )

}
