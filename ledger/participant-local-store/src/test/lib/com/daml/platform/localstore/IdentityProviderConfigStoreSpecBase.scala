// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.resources.TestResourceContext
import com.daml.platform.localstore.api.IdentityProviderConfigStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, AsyncTestSuite, EitherValues, OptionValues}

import scala.concurrent.Future

trait IdentityProviderConfigStoreSpecBase
    extends TestResourceContext
    with Matchers
    with OptionValues
    with EitherValues { self: AsyncTestSuite =>

  def newStore(): IdentityProviderConfigStore

  final protected def testIt(
      f: IdentityProviderConfigStore => Future[Assertion]
  ): Future[Assertion] = f(newStore())

}
