// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore

import com.daml.ledger.resources.TestResourceContext
import com.digitalasset.canton.ledger.api.domain.IdentityProviderConfig
import com.digitalasset.canton.platform.localstore.api.UserManagementStore
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
