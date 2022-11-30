// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.ledger.resources.TestResourceContext
import com.daml.platform.localstore.api.PartyRecordStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, AsyncTestSuite, EitherValues, OptionValues}

import scala.concurrent.Future

trait PartyRecordStoreSpecBase
    extends TestResourceContext
    with Matchers
    with OptionValues
    with EitherValues { self: AsyncTestSuite =>

  def newStore(): PartyRecordStore

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig): Future[Unit]

  final protected def testIt(
      f: PartyRecordStore => Future[Assertion]
  ): Future[Assertion] = f(
    newStore()
  )
}
