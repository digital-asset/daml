// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.resources.TestResourceContext
import com.daml.platform.localstore.PartyRecordStoreSpecBase.StoreContainer
import com.daml.platform.localstore.api.{IdentityProviderConfigStore, PartyRecordStore}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, AsyncTestSuite, EitherValues, OptionValues}

import scala.concurrent.Future

trait PartyRecordStoreSpecBase
    extends TestResourceContext
    with Matchers
    with OptionValues
    with EitherValues { self: AsyncTestSuite =>

  def newStore(): StoreContainer

  final protected def testIt(
      f: StoreContainer => Future[Assertion]
  ): Future[Assertion] = f(
    newStore()
  )

}

object PartyRecordStoreSpecBase {
  case class StoreContainer(
      partyRecordStore: PartyRecordStore,
      identityProviderConfigStore: IdentityProviderConfigStore,
  )
}
