// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.resources.TestResourceContext
import com.daml.platform.localstore.UserStoreSpecBase.StoreContainer
import com.daml.platform.localstore.api.{IdentityProviderConfigStore, UserManagementStore}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, AsyncTestSuite, EitherValues, OptionValues}

import scala.concurrent.Future

trait UserStoreSpecBase
    extends TestResourceContext
    with Matchers
    with OptionValues
    with EitherValues { self: AsyncTestSuite =>

  def newStore(): StoreContainer

  final protected def testIt(f: StoreContainer => Future[Assertion]): Future[Assertion] = f(
    newStore()
  )

}

object UserStoreSpecBase {
  case class StoreContainer(
      userManagementStore: UserManagementStore,
      identityProviderConfigStore: IdentityProviderConfigStore,
  )
}
