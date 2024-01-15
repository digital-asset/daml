// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.ledger.resources.TestResourceContext
import com.daml.platform.localstore.api.UserManagementStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, AsyncTestSuite, EitherValues, OptionValues}

import scala.concurrent.Future

trait UserStoreSpecBase
    extends TestResourceContext
    with Matchers
    with OptionValues
    with EitherValues { self: AsyncTestSuite =>

  def newStore(): UserManagementStore

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig): Future[Unit]

  final protected def testIt(f: UserManagementStore => Future[Assertion]): Future[Assertion] = f(
    newStore()
  )

}
